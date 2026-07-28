#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <memory>
#include <vector>
#include <string>
#include "processing/parallel_pipeline.hpp"
#include "processing/thread_pool.hpp"

// ParallelPipeline unit tests
// These tests verify batch partitioning, parallel processing correctness,
// indicator calculation across a batch, and edge case handling.

// Helpers

static nlohmann::json make_tick(const std::string& ticker, double price,
                                 int64_t volume = 100000) {
    return nlohmann::json{
        { "ticker",    ticker },
        { "price",     price  },
        { "high",      price * 1.01 },
        { "low",       price * 0.99 },
        { "volume",    volume },
        { "timestamp", "2024-01-15T14:30:00Z" },
        { "source",    "alpha_vantage" }
    };
}

// Builds a batch of ticks for a single ticker with incrementing prices
static std::vector<nlohmann::json> make_ticker_batch(
    const std::string& ticker, double start_price, int count) {

    std::vector<nlohmann::json> ticks;
    ticks.reserve(count);
    for (int i = 0; i < count; ++i) {
        ticks.push_back(make_tick(ticker, start_price + i));
    }
    return ticks;
}

// Fixture

class ParallelPipelineTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use 4 worker threads for all tests
        pool_     = std::make_shared<ThreadPool>(4);
        pipeline_ = std::make_unique<ParallelPipeline>(pool_);
    }

    std::shared_ptr<ThreadPool>        pool_;
    std::unique_ptr<ParallelPipeline>  pipeline_;
};

// Empty batch tests

// Processing an empty batch returns an empty results map with no crash
TEST_F(ParallelPipelineTest, EmptyBatchReturnsEmptyResults) {
    const auto results = pipeline_->process_batch({});
    EXPECT_TRUE(results.empty());
}

// Single ticker tests

// A batch with a single tick produces one result entry with the correct price and tick count
TEST_F(ParallelPipelineTest, SingleTickerSingleTick) {
    const auto results = pipeline_->process_batch({ make_tick("AAPL", 182.63) });

    EXPECT_EQ(results.size(), 1u);
    EXPECT_TRUE(results.count("AAPL") > 0);
    EXPECT_DOUBLE_EQ(results.at("AAPL").last_price, 182.63);
    EXPECT_EQ(results.at("AAPL").tick_count, 1u);
}

// Multiple ticks for one ticker are all counted and the last price reflects the final tick
TEST_F(ParallelPipelineTest, SingleTickerMultipleTicks) {
    auto ticks = make_ticker_batch("TSLA", 200.0, 10);

    const auto results = pipeline_->process_batch(ticks);

    EXPECT_EQ(results.size(), 1u);
    EXPECT_EQ(results.at("TSLA").tick_count, 10u);
    // Last price should be the final tick in the batch
    EXPECT_DOUBLE_EQ(results.at("TSLA").last_price, 209.0);
}

// Multi-ticker tests

// Interleaved ticks from several tickers are partitioned into separate, independently-counted results
TEST_F(ParallelPipelineTest, MultipleTickersProcessedIndependently) {
    std::vector<nlohmann::json> batch;

    // Interleave ticks from three tickers
    for (int i = 0; i < 5; ++i) {
        batch.push_back(make_tick("AAPL",  180.0 + i));
        batch.push_back(make_tick("TSLA",  250.0 + i));
        batch.push_back(make_tick("GOOGL", 130.0 + i));
    }

    const auto results = pipeline_->process_batch(batch);

    // Each ticker should have its own independent result
    EXPECT_EQ(results.size(), 3u);
    EXPECT_TRUE(results.count("AAPL")  > 0);
    EXPECT_TRUE(results.count("TSLA")  > 0);
    EXPECT_TRUE(results.count("GOOGL") > 0);

    // Each ticker should have exactly 5 ticks
    EXPECT_EQ(results.at("AAPL").tick_count,  5u);
    EXPECT_EQ(results.at("TSLA").tick_count,  5u);
    EXPECT_EQ(results.at("GOOGL").tick_count, 5u);
}

// Each ticker's last_price reflects only its own most recent tick, not another ticker's
TEST_F(ParallelPipelineTest, LastPriceIsCorrectPerTicker) {
    std::vector<nlohmann::json> batch = {
        make_tick("AAPL", 180.0),
        make_tick("AAPL", 181.0),
        make_tick("AAPL", 182.0),
        make_tick("TSLA", 250.0),
        make_tick("TSLA", 255.0),
    };

    const auto results = pipeline_->process_batch(batch);

    EXPECT_DOUBLE_EQ(results.at("AAPL").last_price, 182.0);
    EXPECT_DOUBLE_EQ(results.at("TSLA").last_price, 255.0);
}

// Indicator tests

// Fewer than 50 ticks leaves indicators_ready false since SMA-50's window isn't filled
TEST_F(ParallelPipelineTest, IndicatorsNotReadyWithFewTicks) {
    // SMA-50 needs 50 ticks before indicators_ready is true
    auto ticks = make_ticker_batch("AAPL", 180.0, 10);

    const auto results = pipeline_->process_batch(ticks);

    EXPECT_FALSE(results.at("AAPL").indicators_ready);
}

// Exactly 50 ticks fills the SMA-50 window, flipping indicators_ready to true
TEST_F(ParallelPipelineTest, IndicatorsReadyAfterFiftyTicks) {
    // Feed exactly 50 ticks to fill the SMA-50 window
    auto ticks = make_ticker_batch("AAPL", 180.0, 50);

    const auto results = pipeline_->process_batch(ticks);

    EXPECT_TRUE(results.at("AAPL").indicators_ready);
}

// SMA-20 over 20 ticks of a constant price equals that price
TEST_F(ParallelPipelineTest, SMA20CalculatedCorrectly) {
    // Feed 20 ticks all at price 100.0 — SMA-20 should equal 100.0
    auto ticks = make_ticker_batch("AAPL", 100.0, 20);

    // All same price so SMA should equal that price
    for (auto& tick : ticks) {
        tick["price"] = 100.0;
        tick["high"]  = 100.0;
        tick["low"]   = 100.0;
    }

    const auto results = pipeline_->process_batch(ticks);
    EXPECT_NEAR(results.at("AAPL").sma_20, 100.0, 0.001);
}

// Edge case tests

// A tick with an invalid (zero) price is silently skipped rather than crashing the pipeline
TEST_F(ParallelPipelineTest, SkipsTicksWithInvalidPrice) {
    std::vector<nlohmann::json> batch = {
        make_tick("AAPL", 182.0),
        make_tick("AAPL", 0.0),   // invalid — should be skipped
        make_tick("AAPL", 183.0),
    };

    // Should not crash — invalid ticks are skipped silently
    const auto results = pipeline_->process_batch(batch);
    EXPECT_TRUE(results.count("AAPL") > 0);
}

// A tick missing the ticker field is silently skipped, leaving only the valid tickers in the results
TEST_F(ParallelPipelineTest, SkipsTicksMissingTickerField) {
    std::vector<nlohmann::json> batch = {
        make_tick("AAPL", 182.0),
        nlohmann::json{ { "price", 100.0 } }, // missing ticker field
        make_tick("TSLA", 250.0),
    };

    // Should not crash — malformed ticks are skipped
    const auto results = pipeline_->process_batch(batch);
    EXPECT_EQ(results.size(), 2u);
}

// Diagnostic counter tests

// batches_processed() increments by one for each call to process_batch
TEST_F(ParallelPipelineTest, BatchCounterIncrementsPerBatch) {
    EXPECT_EQ(pipeline_->batches_processed(), 0u);

    pipeline_->process_batch({ make_tick("AAPL", 182.0) });
    EXPECT_EQ(pipeline_->batches_processed(), 1u);

    pipeline_->process_batch({ make_tick("TSLA", 250.0) });
    EXPECT_EQ(pipeline_->batches_processed(), 2u);
}

// ticks_processed() accumulates the total number of ticks across all processed batches
TEST_F(ParallelPipelineTest, TickCounterTracksAllTicks) {
    EXPECT_EQ(pipeline_->ticks_processed(), 0u);

    pipeline_->process_batch({
        make_tick("AAPL", 182.0),
        make_tick("TSLA", 250.0),
        make_tick("GOOGL", 140.0)
    });

    EXPECT_EQ(pipeline_->ticks_processed(), 3u);
}

// Two pipelines sharing the same ThreadPool both process their batches correctly and independently
TEST_F(ParallelPipelineTest, ThreadPoolSharedCorrectly) {
    // Both pipeline and a second pipeline share the same pool
    auto pipeline2 = std::make_unique<ParallelPipeline>(pool_);

    // Both should work correctly with the shared pool
    auto r1 = pipeline_->process_batch({ make_tick("AAPL", 182.0) });
    auto r2 = pipeline2->process_batch({ make_tick("TSLA", 250.0) });

    EXPECT_TRUE(r1.count("AAPL") > 0);
    EXPECT_TRUE(r2.count("TSLA") > 0);
}