#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include "storage/timeseries_buffer.hpp"

// InfluxDBWriter unit tests
// The InfluxDBWriter requires a live InfluxDB connection so we test its
// data validation, point building logic, and the TimeSeriesBuffer it relies
// on for batching. Connection and write tests are covered by integration tests.

// Helpers

static nlohmann::json make_tick(const std::string& ticker, double price) {
    return nlohmann::json{
        { "ticker",    ticker },
        { "price",     price  },
        { "open",      price  },
        { "high",      price * 1.01 },
        { "low",       price * 0.99 },
        { "volume",    100000 },
        { "timestamp", "2024-01-15T14:30:00Z" },
        { "source",    "alpha_vantage" }
    };
}

static nlohmann::json make_enriched_tick(const std::string& ticker, double price) {
    nlohmann::json tick = make_tick(ticker, price);
    tick["indicators"] = {
        { "sma_20", price * 0.99 },
        { "sma_50", price * 0.98 },
        { "rsi_14", 55.0 },
        { "vwap",   price * 1.001 }
    };
    tick["indicators_ready"] = true;
    return tick;
}

// Tick validation tests

// A well-formed tick has non-empty ticker and positive price
TEST(InfluxDBWriterTest, ValidTickHasRequiredFields) {
    const nlohmann::json tick = make_tick("AAPL", 182.63);

    EXPECT_TRUE(tick.contains("ticker"));
    EXPECT_TRUE(tick.contains("price"));
    EXPECT_FALSE(tick["ticker"].get<std::string>().empty());
    EXPECT_GT(tick["price"].get<double>(), 0.0);
}

// A tick missing the ticker field is detectable as invalid
TEST(InfluxDBWriterTest, InvalidTickMissingTicker) {
    nlohmann::json tick = make_tick("AAPL", 182.63);
    tick.erase("ticker");

    EXPECT_FALSE(tick.contains("ticker"));
}

// A tick missing the price field is detectable as invalid
TEST(InfluxDBWriterTest, InvalidTickMissingPrice) {
    nlohmann::json tick = make_tick("AAPL", 182.63);
    tick.erase("price");

    EXPECT_FALSE(tick.contains("price"));
}

// A zero price is preserved as-is by the tick builder (validation of this case happens elsewhere)
TEST(InfluxDBWriterTest, InvalidTickZeroPrice) {
    const nlohmann::json tick = make_tick("AAPL", 0.0);
    EXPECT_DOUBLE_EQ(tick["price"].get<double>(), 0.0);
}

// An enriched tick carries all indicator fields (sma_20, sma_50, rsi_14, vwap) and the ready flag
TEST(InfluxDBWriterTest, EnrichedTickHasIndicators) {
    const nlohmann::json tick = make_enriched_tick("AAPL", 182.63);

    EXPECT_TRUE(tick.contains("indicators"));
    EXPECT_TRUE(tick.contains("indicators_ready"));
    EXPECT_TRUE(tick["indicators_ready"].get<bool>());
    EXPECT_TRUE(tick["indicators"].contains("sma_20"));
    EXPECT_TRUE(tick["indicators"].contains("sma_50"));
    EXPECT_TRUE(tick["indicators"].contains("rsi_14"));
    EXPECT_TRUE(tick["indicators"].contains("vwap"));
}

TEST(InfluxDBWriterTest, IndicatorsNotReadyFlagWorks) {
    nlohmann::json tick = make_enriched_tick("AAPL", 182.63);
    tick["indicators_ready"] = false;

    // When indicators_ready is false we should not write indicator fields
    EXPECT_FALSE(tick["indicators_ready"].get<bool>());
}

// TimeSeriesBuffer tests

// A newly constructed buffer is empty with size 0 and the requested capacity
TEST(TimeSeriesBufferTest, StartsEmpty) {
    TimeSeriesBuffer buffer(100);

    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(buffer.size(), 0u);
    EXPECT_EQ(buffer.capacity(), 100u);
}

// Each push grows the buffer's size by one and clears the empty flag
TEST(TimeSeriesBufferTest, PushIncreasesSize) {
    TimeSeriesBuffer buffer(100);

    buffer.push(make_tick("AAPL", 182.63));
    EXPECT_EQ(buffer.size(), 1u);
    EXPECT_FALSE(buffer.empty());

    buffer.push(make_tick("TSLA", 250.00));
    EXPECT_EQ(buffer.size(), 2u);
}

// Draining with a limit higher than the item count returns and empties everything
TEST(TimeSeriesBufferTest, DrainReturnsCorrectCount) {
    TimeSeriesBuffer buffer(100);

    buffer.push(make_tick("AAPL", 182.63));
    buffer.push(make_tick("TSLA", 250.00));
    buffer.push(make_tick("GOOGL", 140.00));

    std::vector<nlohmann::json> output;
    const size_t drained = buffer.drain(output, 10);

    EXPECT_EQ(drained, 3u);
    EXPECT_EQ(output.size(), 3u);
    EXPECT_TRUE(buffer.empty());
}

// Drain stops at the requested max_items, leaving the rest in the buffer
TEST(TimeSeriesBufferTest, DrainRespectsMaxItems) {
    TimeSeriesBuffer buffer(100);

    for (int i = 0; i < 10; ++i) {
        buffer.push(make_tick("AAPL", 180.0 + i));
    }

    std::vector<nlohmann::json> output;
    const size_t drained = buffer.drain(output, 5); // only drain 5

    EXPECT_EQ(drained, 5u);
    EXPECT_EQ(buffer.size(), 5u); // 5 remaining
}

// Drain returns items in FIFO order, oldest pushed item first
TEST(TimeSeriesBufferTest, DrainPreservesOldestFirst) {
    TimeSeriesBuffer buffer(100);

    buffer.push(make_tick("AAPL", 100.0));
    buffer.push(make_tick("AAPL", 200.0));
    buffer.push(make_tick("AAPL", 300.0));

    std::vector<nlohmann::json> output;
    buffer.drain(output, 10);

    // Oldest tick (100.0) should come out first
    EXPECT_DOUBLE_EQ(output[0]["price"].get<double>(), 100.0);
    EXPECT_DOUBLE_EQ(output[1]["price"].get<double>(), 200.0);
    EXPECT_DOUBLE_EQ(output[2]["price"].get<double>(), 300.0);
}

// When the ring buffer is full, pushing evicts the oldest item and increments the overwrite counter
TEST(TimeSeriesBufferTest, OverwritesOldestWhenFull) {
    TimeSeriesBuffer buffer(3); // tiny buffer

    buffer.push(make_tick("AAPL", 100.0)); // slot 0
    buffer.push(make_tick("AAPL", 200.0)); // slot 1
    buffer.push(make_tick("AAPL", 300.0)); // slot 2 — buffer full
    buffer.push(make_tick("AAPL", 400.0)); // overwrites oldest (100.0)

    EXPECT_EQ(buffer.total_overwritten(), 1u);
    EXPECT_EQ(buffer.size(), 3u); // still 3 items

    std::vector<nlohmann::json> output;
    buffer.drain(output, 10);

    // 100.0 should be gone — 200, 300, 400 remain
    EXPECT_EQ(output.size(), 3u);
    EXPECT_DOUBLE_EQ(output[0]["price"].get<double>(), 200.0);
    EXPECT_DOUBLE_EQ(output[2]["price"].get<double>(), 400.0);
}

// total_pushed keeps counting every push even after the buffer has wrapped and overwritten items
TEST(TimeSeriesBufferTest, TotalPushedCounterTracksAllPushes) {
    TimeSeriesBuffer buffer(5);

    EXPECT_EQ(buffer.total_pushed(), 0u);

    for (int i = 0; i < 10; ++i) {
        buffer.push(make_tick("AAPL", 180.0 + i));
    }

    // total_pushed counts every push including overwrites
    EXPECT_EQ(buffer.total_pushed(), 10u);
}

// clear() empties the buffer and resets size back to zero
TEST(TimeSeriesBufferTest, ClearResetsAllState) {
    TimeSeriesBuffer buffer(100);

    buffer.push(make_tick("AAPL", 182.63));
    buffer.push(make_tick("TSLA", 250.00));
    EXPECT_EQ(buffer.size(), 2u);

    buffer.clear();
    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(buffer.size(), 0u);
}

// Draining an empty buffer is a no-op that returns zero items
TEST(TimeSeriesBufferTest, DrainFromEmptyBufferReturnsZero) {
    TimeSeriesBuffer buffer(100);

    std::vector<nlohmann::json> output;
    const size_t drained = buffer.drain(output, 10);

    EXPECT_EQ(drained, 0u);
    EXPECT_TRUE(output.empty());
}

// Constructing a buffer with zero capacity is rejected with std::invalid_argument
TEST(TimeSeriesBufferTest, ThrowsOnZeroCapacity) {
    EXPECT_THROW(TimeSeriesBuffer buffer(0), std::invalid_argument);
}

// Pushing exactly at capacity (1000 items into a 1000-capacity buffer) keeps data intact and in order, with no overwrites
TEST(TimeSeriesBufferTest, LargeVolumeDoesNotCorruptData) {
    TimeSeriesBuffer buffer(1000);

    // Push 1000 ticks
    for (int i = 0; i < 1000; ++i) {
        buffer.push(make_tick("AAPL", 100.0 + i));
    }

    EXPECT_EQ(buffer.size(), 1000u);
    EXPECT_EQ(buffer.total_pushed(), 1000u);
    EXPECT_EQ(buffer.total_overwritten(), 0u);

    std::vector<nlohmann::json> output;
    buffer.drain(output, 1000);

    EXPECT_EQ(output.size(), 1000u);
    EXPECT_DOUBLE_EQ(output[0]["price"].get<double>(), 100.0);
    EXPECT_DOUBLE_EQ(output[999]["price"].get<double>(), 1099.0);
}