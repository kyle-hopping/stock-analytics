#include "stock_fetcher.hpp"
#include <stdexcept>
#include <ctime>
#include <thread>
#include <sstream>
#include <iomanip>

StockFetcher::StockFetcher() {
    // Load API keys from env variables
    const char* av_key = std::getenv("ALPHA_VANTAGE_KEY");
    if (!av_key || std::string(av_key).empty()) {
        throw std::runtime_error("StockFetcher: ALPHA_VANTAGE_KEY environment variable not set");
    }
    alpha_vantage_key_ = av_key;

    const char* massive_key = std::getenv("MASSIVE_API_KEY");
    if (!massive_key || std::string(massive_key).empty()) {
        throw std::runtime_error("StockFetcher: MASSIVE_API_KEY environment variable not set");
    }
    massive_api_key_ = massive_key;

    // Initialize libcurl
    // curl_global_init must be called once before any curl operations.
    // CURL_GLOBAL_DEFAULT initializes both SSL and Winsock.
    curl_global_init(CURL_GLOBAL_DEFAULT);

    curl_ = curl_easy_init();
    if (!curl_) {
        throw std::runtime_error("StockFetcher: failed to initialize libcurl");
    }

    // Follow HTTP redirects automatically — some API endpoints redirect
    curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1L);

    // Timeout after 10 seconds to prevent hanging on slow API responses
    curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 10L);

    // Register our write callback to capture the HTTP response body
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, curl_write_callback);
    spdlog::info("StockFetcher: initialized with Alpha Vantage and Massive API keys");
}

StockFetcher::~StockFetcher() {
    stop();

    if (curl_) {
        curl_easy_cleanup(curl_);
        curl_global_cleanup();
    }
}

void StockFetcher::fetch_alpha_vantage() {
    if (!callback_) {
        throw std::runtime_error("StockFetcher: no callback registered — call set_callback() first");
    }

    if (tickers_.empty()) {
        spdlog::warn("StockFetcher: no tickers added — call add_ticker() first");
        return;
    }

    spdlog::info("StockFetcher: fetching {} ticker(s) from Alpha Vantage", tickers_.size());

    for (const auto& ticker : tickers_) {
        if (!running_.load()) {
            break; // stop() was called mid-fetch
        }

        const std::string url = build_alpha_vantage_url(ticker);
        spdlog::debug("StockFetcher: GET {}", url);

        // Perform the HTTP request — blocks until response or timeout
        const std::string response_body = http_get(url);
        if (response_body.empty()) {
            spdlog::error("StockFetcher: empty response for ticker '{}'", ticker);
            continue;
        }

        nlohmann::json response;
        try {
            response = nlohmann::json::parse(response_body);
        } catch (const nlohmann::json::parse_error& e) {
            spdlog::error("StockFetcher: JSON parse error for '{}': {}", ticker, e.what());
            continue;
        }

        // Check for API error messages
        if (response.contains("Note") || response.contains("Information")) {
            spdlog::warn("StockFetcher: Alpha Vantage rate limit hit for '{}' — "
                "consider upgrading your API plan", ticker);
            continue;
        }

        // Normalize the response into the common tick format
        const nlohmann::json tick = parse_alpha_vantage_quote(ticker, response);
        if (tick.empty()) {
            spdlog::warn("StockFetcher: could not parse quote for '{}'", ticker);
            continue;
        }

        spdlog::info("StockFetcher: received tick for {} @ ${:.2f}",
            ticker, tick["price"].get<double>());

        // Dispatch the normalized tick to the callback (typically KafkaProducer)
        callback_(ticker, tick);

        // Rate limit — wait between requests to stay within API limits.
        // Alpha Vantage free tier: 25 requests/day, premium: 75+/min.
        if (&ticker != &tickers_.back()) {
            spdlog::debug("StockFetcher: rate limit delay {}ms", ALPHA_VANTAGE_RATE_LIMIT_MS);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(ALPHA_VANTAGE_RATE_LIMIT_MS)
            );
        }
    }
}

void StockFetcher::start_polling(std::chrono::seconds interval) {
    running_.store(true);
    spdlog::info("StockFetcher: starting poll loop with {}s interval", interval.count());

    while (running_.load()) {
        fetch_alpha_vantage();

        // Sleep for the configured interval between full fetch cycles.
        // We sleep in 1-second chunks so stop() is responsive.
        for (int i = 0; i < interval.count() && running_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    spdlog::info("StockFetcher: poll loop stopped");
}

std::string StockFetcher::http_get(const std::string& url) {
    std::string response_body;

    // Pass a pointer to our local string as the write callback's userdata
    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response_body);

    const CURLcode res = curl_easy_perform(curl_);
    if (res != CURLE_OK) {
        spdlog::error("StockFetcher: HTTP request failed: {}", curl_easy_strerror(res));
        return {};
    }

    // Check the HTTP status code — anything outside 200-299 is an error
    long http_code = 0;
    curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_code);
    if (http_code < 200 || http_code >= 300) {
        spdlog::error("StockFetcher: HTTP {} for URL: {}", http_code, url);
        return {};
    }

    return response_body;
}

nlohmann::json StockFetcher::parse_alpha_vantage_quote(const std::string& ticker,
                                                        const nlohmann::json& response) {
    // Alpha Vantage global quote response structure:
    // { "Global Quote": { "01. symbol": "AAPL", "05. price": "182.63", ... } }
    if (!response.contains("Global Quote")) {
        spdlog::error("StockFetcher: missing 'Global Quote' key in Alpha Vantage response");
        return {};
    }

    const auto& quote = response["Global Quote"];

    // Validate all required fields are present before accessing them
    const std::vector<std::string> required = {
        "05. price", "02. open", "03. high", "04. low", "06. volume"
    };
    for (const auto& field : required) {
        if (!quote.contains(field)) {
            spdlog::error("StockFetcher: missing field '{}' in Alpha Vantage quote", field);
            return {};
        }
    }

    // Build the normalized tick — parse string values from Alpha Vantage
    // (their API returns all numbers as strings) into proper numeric types
    return nlohmann::json{
        { "ticker",    ticker },
        { "price",     std::stod(quote["05. price"].get<std::string>()) },
        { "open",      std::stod(quote["02. open"].get<std::string>()) },
        { "high",      std::stod(quote["03. high"].get<std::string>()) },
        { "low",       std::stod(quote["04. low"].get<std::string>()) },
        { "volume",    std::stoll(quote["06. volume"].get<std::string>()) },
        { "timestamp", current_timestamp() },
        { "source",    "alpha_vantage" }
    };
}

nlohmann::json StockFetcher::parse_massive_quote(const std::string& ticker,
                                                  const nlohmann::json& response) {
    // Massive (Polygon.io) last trade response structure:
    // { "results": { "p": 182.63, "s": 100, "t": 1705329000000 }, "status": "OK" }
    if (!response.contains("results") || !response.contains("status")) {
        spdlog::error("StockFetcher: unexpected Massive response structure for '{}'", ticker);
        return {};
    }

    if (response["status"] != "OK") {
        spdlog::error("StockFetcher: Massive API returned status '{}' for '{}'",
            response["status"].get<std::string>(), ticker);
        return {};
    }

    const auto& result = response["results"];

    // Build normalized tick from Massive response fields:
    // p = price, s = size (volume), t = timestamp (Unix ms)
    return nlohmann::json{
        { "ticker",    ticker },
        { "price",     result.value("p", 0.0) },
        { "open",      result.value("p", 0.0) },
        { "high",      result.value("p", 0.0) },
        { "low",       result.value("p", 0.0) },
        { "volume",    result.value("s", 0) },
        { "timestamp", current_timestamp() },
        { "source",    "massive" }
    };
}

size_t StockFetcher::curl_write_callback(char* ptr, size_t size,
                                          size_t nmemb, void* userdata) {
    // libcurl calls this function each time it receives a chunk of response data.
    // We append each chunk to the response string until the full body is received.
    const size_t total_bytes = size * nmemb;
    auto* response = static_cast<std::string*>(userdata);
    response->append(ptr, total_bytes);
    return total_bytes;
}

std::string StockFetcher::build_alpha_vantage_url(const std::string& ticker) const {
    // Alpha Vantage global quote endpoint — returns latest price data
    return "https://www.alphavantage.co/query"
           "?function=GLOBAL_QUOTE"
           "&symbol=" + ticker +
           "&apikey=" + alpha_vantage_key_;
}

std::string StockFetcher::build_massive_url(const std::string& ticker) const {
    // Massive (Polygon.io) last trade endpoint
    return "https://api.polygon.io/v2/last/trade/" + ticker +
           "?apiKey=" + massive_api_key_;
}

std::string StockFetcher::current_timestamp() {
    // Generate an ISO 8601 UTC timestamp for the tick record.
    // Example: "2024-01-15T14:30:00Z"
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm utc_tm{};

    // Use gmtime_s on Windows (thread-safe — gmtime is supposedly not)
    #ifdef _WIN32
        gmtime_s(&utc_tm, &t);
    #else
        gmtime_r(&t, &utc_tm);
    #endif

    std::ostringstream oss;
    oss << std::put_time(&utc_tm, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}