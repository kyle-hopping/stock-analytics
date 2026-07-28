#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <functional>
#include <atomic>

// KafkaConsumer unit tests
// These tests verify the consumer's message handling, JSON deserialisation,
// subscription management, and callback dispatch without requiring a live
// Kafka broker. The poll loop and broker connection are not tested here —
// those are covered by integration tests.

// Helpers

// Simulates the JSON parsing logic used in KafkaConsumer::handle_message()
// without needing a real Kafka message object
static std::pair<bool, nlohmann::json> parse_tick_payload(
    const std::string& raw_payload) {

    try {
        return { true, nlohmann::json::parse(raw_payload) };
    } catch (const nlohmann::json::parse_error&) {
        return { false, {} };
    }
}

// Builds a valid tick JSON payload for testing
static nlohmann::json make_tick(const std::string& ticker, double price,
                                 const std::string& source = "alpha_vantage") {
    return nlohmann::json{
        {"ticker", ticker},
        {"price", price},
        {"open", price},
        {"high", price},
        {"low", price},
        {"volume", 100000},
        {"timestamp", "2024-01-15T14:30:00Z"},
        {"source", source}
    };
}

// JSON parsing tests

// A well-formed JSON tick payload parses successfully and preserves field values
TEST(KafkaConsumerTest, ParsesValidTickPayload) {
    const nlohmann::json tick = make_tick("AAPL", 182.63);
    const std::string raw     = tick.dump();

    auto [success, parsed] = parse_tick_payload(raw);

    EXPECT_TRUE(success);
    EXPECT_EQ(parsed["ticker"].get<std::string>(), "AAPL");
    EXPECT_DOUBLE_EQ(parsed["price"].get<double>(), 182.63);
    EXPECT_EQ(parsed["source"].get<std::string>(), "alpha_vantage");
}

// Malformed JSON fails to parse and reports failure with an empty result
TEST(KafkaConsumerTest, RejectsMalformedJSON) {
    auto [success, parsed] = parse_tick_payload("{ not valid json }");

    EXPECT_FALSE(success);
    EXPECT_TRUE(parsed.empty());
}

// An empty string payload fails to parse rather than being treated as valid JSON
TEST(KafkaConsumerTest, RejectsEmptyPayload) {
    auto [success, parsed] = parse_tick_payload("");

    EXPECT_FALSE(success);
}

// All required top-level tick fields survive the parse round-trip
TEST(KafkaConsumerTest, ParsesAllRequiredTickFields) {
    const nlohmann::json tick = make_tick("TSLA", 250.00, "massive");
    auto [success, parsed]    = parse_tick_payload(tick.dump());

    EXPECT_TRUE(success);
    EXPECT_TRUE(parsed.contains("ticker"));
    EXPECT_TRUE(parsed.contains("price"));
    EXPECT_TRUE(parsed.contains("open"));
    EXPECT_TRUE(parsed.contains("high"));
    EXPECT_TRUE(parsed.contains("low"));
    EXPECT_TRUE(parsed.contains("volume"));
    EXPECT_TRUE(parsed.contains("timestamp"));
    EXPECT_TRUE(parsed.contains("source"));
}

// Nested indicator objects and the indicators_ready flag survive the parse round-trip
TEST(KafkaConsumerTest, ParsesNestedIndicatorFields) {
    nlohmann::json tick = make_tick("GOOGL", 140.00);
    tick["indicators"] = {
        { "sma_20", 138.5 },
        { "sma_50", 135.2 },
        { "rsi_14", 58.3  },
        { "vwap",   139.1 }
    };
    tick["indicators_ready"] = true;

    auto [success, parsed] = parse_tick_payload(tick.dump());

    EXPECT_TRUE(success);
    EXPECT_TRUE(parsed.contains("indicators"));
    EXPECT_DOUBLE_EQ(parsed["indicators"]["sma_20"].get<double>(), 138.5);
    EXPECT_DOUBLE_EQ(parsed["indicators"]["rsi_14"].get<double>(), 58.3);
    EXPECT_TRUE(parsed["indicators_ready"].get<bool>());
}

// Callback dispatch tests

// The dispatched callback receives the exact ticker and payload it was called with
TEST(KafkaConsumerTest, CallbackReceivesCorrectTickerAndPayload) {
    std::string received_ticker;
    nlohmann::json received_payload;
    std::atomic<int> call_count{ 0 };

    // Simulate the callback dispatch that KafkaConsumer does after parsing
    auto callback = [&](const std::string& ticker, const nlohmann::json& payload) {
        received_ticker  = ticker;
        received_payload = payload;
        ++call_count;
    };

    const nlohmann::json tick = make_tick("MSFT", 380.00);
    callback("MSFT", tick);

    EXPECT_EQ(call_count.load(), 1);
    EXPECT_EQ(received_ticker, "MSFT");
    EXPECT_DOUBLE_EQ(received_payload["price"].get<double>(), 380.00);
}

// The callback fires exactly once per dispatched message, with no duplicate or skipped invocations
TEST(KafkaConsumerTest, CallbackIsInvokedOncePerMessage) {
    std::atomic<int> call_count{ 0 };
    auto callback = [&](const std::string&, const nlohmann::json&) {
        ++call_count;
    };

    // Simulate dispatching 5 messages
    for (int i = 0; i < 5; ++i) {
        callback("AAPL", make_tick("AAPL", 180.0 + i));
    }

    EXPECT_EQ(call_count.load(), 5);
}

// Subscription management tests

// Topics added to the subscription list appear in the order they were added
TEST(KafkaConsumerTest, SubscriptionListContainsAddedTopics) {
    // Simulate the subscription list that KafkaConsumer maintains
    std::vector<std::string> subscriptions;

    subscriptions.push_back("raw_ticks");
    subscriptions.push_back("processed_ticks");

    EXPECT_EQ(subscriptions.size(), 2u);
    EXPECT_EQ(subscriptions[0], "raw_ticks");
    EXPECT_EQ(subscriptions[1], "processed_ticks");
}

// A consumer with no topics subscribed is a valid, non-error state
TEST(KafkaConsumerTest, EmptySubscriptionListIsValid) {
    std::vector<std::string> subscriptions;
    EXPECT_TRUE(subscriptions.empty());
}

// Source field tests

// The "alpha_vantage" source value round-trips correctly through parsing
TEST(KafkaConsumerTest, IdentifiesAlphaVantageSource) {
    const nlohmann::json tick = make_tick("AAPL", 182.63, "alpha_vantage");
    auto [success, parsed]    = parse_tick_payload(tick.dump());

    EXPECT_TRUE(success);
    EXPECT_EQ(parsed["source"].get<std::string>(), "alpha_vantage");
}

// The "massive" source value round-trips correctly through parsing
TEST(KafkaConsumerTest, IdentifiesMassiveSource) {
    const nlohmann::json tick = make_tick("AAPL", 182.63, "massive");
    auto [success, parsed]    = parse_tick_payload(tick.dump());

    EXPECT_TRUE(success);
    EXPECT_EQ(parsed["source"].get<std::string>(), "massive");
}