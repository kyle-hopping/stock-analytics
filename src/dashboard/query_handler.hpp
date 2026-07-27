#pragma once
#include "../storage/influxdb_writer.hpp"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <InfluxDB/InfluxDBFactory.h>
#include <InfluxDB/InfluxDB.h>
#include <memory>
#include <string>
#include <vector>

// QueryHandler reads historical and aggregated stock data from InfluxDB
// and formats it as JSON for delivery to the dashboard WebSocket clients.
// It is the read-side complement to InfluxDBWriter's write-side operations.

// Supported query types:
//   - Latest tick for a given ticker
//   - Price history over a time range
//   - Indicator history (SMA, RSI, VWAP) over a time range
//   - Top movers — tickers with largest price change in a window

class QueryHandler {
public:
    // Constructs the query handler using the same environment variables
    // as InfluxDBWriter — INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET.
    // Throws std::runtime_error if connection fails.
    QueryHandler();

    // Returns the latest tick data for a given ticker as JSON.
    // Returns empty JSON object if no data is found.
    // Example:
    //   { "ticker": "AAPL", "price": 182.63, "rsi_14": 62.3, ... }
    nlohmann::json get_latest_tick(const std::string& ticker);

    // Returns price history for a ticker over the given time range.
    // range — InfluxDB duration string e.g. "1h", "24h", "7d"
    // Returns a JSON array of price points with timestamps.
    nlohmann::json get_price_history(const std::string& ticker,
                                     const std::string& range = "1h");

    // Returns indicator history for a ticker over the given time range.
    // Includes SMA-20, SMA-50, RSI-14, and VWAP values.
    nlohmann::json get_indicator_history(const std::string& ticker,
                                          const std::string& range = "1h");

    // Returns the top N tickers by absolute price change over the given range.
    // Useful for a "top movers" widget on the dashboard.
    nlohmann::json get_top_movers(int top_n = 10,
                                  const std::string& range = "1h");

    // Returns a summary of all tracked tickers with their latest prices.
    nlohmann::json get_market_summary();

    // Returns true if the query handler is connected to InfluxDB.
    bool is_connected() const { return db_ != nullptr; }

private:
    // Executes a Flux query against InfluxDB and returns the raw response.
    // Returns empty string on failure.
    std::string execute_query(const std::string& flux_query);

    // Parses a raw InfluxDB query response (CSV format) into a JSON array.
    nlohmann::json parse_query_response(const std::string& response);

    std::unique_ptr<influxdb::InfluxDB> db_; // InfluxDB read connection
    std::string bucket_;                      // bucket name for queries
    std::string org_;                         // org name for queries
};