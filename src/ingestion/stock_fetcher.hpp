#pragma once
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <curl/curl.h>
#include <atomic>
#include <functional>
#include <string>
#include <vector>
#include <chrono>

// StockFetcher polls stock price data from two sources:
// Alpha Vantage REST API — reliable OHLCV data, 25 req/day bc im poor
// Massive (Polygon.io) — better for real-time WebSocket tick data
// It normalizes both sources into a common JSON tick format and publishes
// each tick to a user-supplied callback (typically the KafkaProducer).

// Sample tick format published to callback:
// {
//   "ticker":  "AAPL",
//   "price":   184.63,
//   "open":    187.20,
//   "high":    204.10,
//   "low":     180.95,
//   "volume":  59999990,
//   "timestamp": "2002-01-07T14:30:00Z",
//   "source":  "alpha_vantage" | "massive"
// }
class StockFetcher {
public:
    // Callback invoked for every tick received from either source.
    // Receives the ticker symbol and the normalized JSON tick payload.
    using TickCallback = std::function<void(const std::string& ticker,
                                            const nlohmann::json& tick)>;

    // Constructor - fetches API credentials loaded from environment
    // variables. initializes the libcurl session. Throws std::runtime_error if
    // libcurl cannot be initialized or if required env variables are missing.
    StockFetcher();

    // Destructor - cleans up the libcurl session.
    ~StockFetcher();

    // Registers the cb invoked for every tick received. Has to be set before
    // calling fetch_alpha_vantage() or start_polling().
    void set_callback(TickCallback callback) { callback_ = std::move(callback); }

    // Adds a ticker symbol to the watch list.
    void add_ticker(const std::string& ticker) { tickers_.push_back(ticker); }

    // Fetches the latest quote for all watched tickers from Alpha Vantage.
    // Respects the API rate limit. Should be called on its own thread as it
    // blocks during HTTP requests.
    void fetch_alpha_vantage();

    // Starts a polling loop that calls fetch_alpha_vantage() repeatedly at
    // the given interval. Blocks the calling thread until stop() is called.
    void start_polling(std::chrono::seconds interval = std::chrono::seconds(60));

    // Signals the polling loop to stop after the current fetch completes.
    // Safe to call from any thread.
    void stop() { running_.store(false); }

    // Returns true if the polling loop is currently running.
    bool is_running() const { return running_.load(); }

private:
    // Performs a blocking HTTP GET request to the given URL.
    // Returns the response as a string, or empty string on failure.
    std::string http_get(const std::string& url);

    // Parses an Alpha Vantage global quote response and normalizes it
    // into the common tick format. Returns empty json on failure.
    nlohmann::json parse_alpha_vantage_quote(const std::string& ticker,
                                              const nlohmann::json& response);

    // Parses a Massive (Polygon.io) trade response and normalizes it
    // into the common tick format. Returns empty json on failure.
    nlohmann::json parse_massive_quote(const std::string& ticker,
                                       const nlohmann::json& response);

    // libcurl write callback — appends received data chunks to a std::string.
    // Must be static as libcurl calls it as a plain C function pointer.
    static size_t curl_write_callback(char* ptr, size_t size,
                                      size_t nmemb, void* userdata);

    // Builds the Alpha Vantage API URL for the global quote endpoint.
    std::string build_alpha_vantage_url(const std::string& ticker) const;

    // Builds the Massive (Polygon.io) API URL for the last trade endpoint.
    std::string build_massive_url(const std::string& ticker) const;

    // Returns the current UTC timestamp as an ISO 8601 string.
    static std::string current_timestamp();

    CURL* curl_;                         // libcurl easy handle
    TickCallback callback_;              // user-supplied tick handler
    std::vector<std::string> tickers_;   // list of symbols to watch
    std::atomic<bool> running_{ false }; // controls the polling loop
    std::string alpha_vantage_key_;      // Alpha Vantage API key from env
    std::string massive_api_key_;        // Massive API key from env

    // Min delay between Alpha Vantage requests to stay within rate limits.
    // Free tier allows 25 requests/day.
    static constexpr int ALPHA_VANTAGE_RATE_LIMIT_MS = 20000;
};