#include "query_handler.hpp"
#include <sstream>
#include <stdexcept>

QueryHandler::QueryHandler() {
    // Load connection parameters from environment
    const char* url_env    = std::getenv("INFLUXDB_URL");
    const char* token_env  = std::getenv("INFLUXDB_TOKEN");
    const char* org_env    = std::getenv("INFLUXDB_ORG");
    const char* bucket_env = std::getenv("INFLUXDB_BUCKET");

    if (!url_env || !token_env || !org_env || !bucket_env) {
        throw std::runtime_error(
            "QueryHandler: missing required environment variables. "
            "Ensure INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, "
            "and INFLUXDB_BUCKET are all set."
        );
    }

    bucket_ = bucket_env;
    org_    = org_env;

    const std::string connection_url =
        std::string(url_env) +
        "?org="    + org_    +
        "&bucket=" + bucket_ +
        "&token="  + token_env;

    try {
        db_ = influxdb::InfluxDBFactory::Get(connection_url);
        spdlog::info("QueryHandler: connected to InfluxDB bucket '{}'", bucket_);
    } catch (const std::exception& e) {
        throw std::runtime_error(
            "QueryHandler: failed to connect to InfluxDB: " +
            std::string(e.what())
        );
    }
}

nlohmann::json QueryHandler::get_latest_tick(const std::string& ticker) {
    // Flux query — fetch the most recent record for this ticker
    const std::string query =
        "from(bucket: \"" + bucket_ + "\")\n"
        "  |> range(start: -1h)\n"
        "  |> filter(fn: (r) => r._measurement == \"stock_ticks\")\n"
        "  |> filter(fn: (r) => r.ticker == \"" + ticker + "\")\n"
        "  |> last()\n"
        "  |> pivot(rowKey:[\"_time\"], columnKey:[\"_field\"], valueColumn:\"_value\")";

    const std::string response = execute_query(query);
    if (response.empty()) {
        spdlog::warn("QueryHandler: no data found for ticker '{}'", ticker);
        return {};
    }

    nlohmann::json result = parse_query_response(response);

    // Return the first (and only) record, or empty object if none found
    if (result.is_array() && !result.empty()) {
        return result[0];
    }

    return {};
}

nlohmann::json QueryHandler::get_price_history(const std::string& ticker,
                                                const std::string& range) {
    // Flux query — fetch price field over the given time range
    const std::string query =
        "from(bucket: \"" + bucket_ + "\")\n"
        "  |> range(start: -" + range + ")\n"
        "  |> filter(fn: (r) => r._measurement == \"stock_ticks\")\n"
        "  |> filter(fn: (r) => r.ticker == \"" + ticker + "\")\n"
        "  |> filter(fn: (r) => r._field == \"price\")\n"
        "  |> sort(columns: [\"_time\"])";

    const std::string response = execute_query(query);
    if (response.empty()) {
        return nlohmann::json::array();
    }

    return parse_query_response(response);
}

nlohmann::json QueryHandler::get_indicator_history(const std::string& ticker,
                                                     const std::string& range) {
    // Flux query — fetch all indicator fields pivoted into columns
    const std::string query =
        "from(bucket: \"" + bucket_ + "\")\n"
        "  |> range(start: -" + range + ")\n"
        "  |> filter(fn: (r) => r._measurement == \"stock_ticks\")\n"
        "  |> filter(fn: (r) => r.ticker == \"" + ticker + "\")\n"
        "  |> filter(fn: (r) =>\n"
        "      r._field == \"sma_20\" or r._field == \"sma_50\" or\n"
        "      r._field == \"rsi_14\" or r._field == \"vwap\")\n"
        "  |> pivot(rowKey:[\"_time\"], columnKey:[\"_field\"], valueColumn:\"_value\")\n"
        "  |> sort(columns: [\"_time\"])";

    const std::string response = execute_query(query);
    if (response.empty()) {
        return nlohmann::json::array();
    }

    return parse_query_response(response);
}

nlohmann::json QueryHandler::get_top_movers(int top_n, const std::string& range) {
    // Flux query — compute price change % per ticker and return top N
    const std::string query =
        "first_prices = from(bucket: \"" + bucket_ + "\")\n"
        "  |> range(start: -" + range + ")\n"
        "  |> filter(fn: (r) => r._measurement == \"stock_ticks\" and r._field == \"price\")\n"
        "  |> first()\n"
        "  |> rename(columns: {_value: \"first_price\"})\n"
        "\n"
        "last_prices = from(bucket: \"" + bucket_ + "\")\n"
        "  |> range(start: -" + range + ")\n"
        "  |> filter(fn: (r) => r._measurement == \"stock_ticks\" and r._field == \"price\")\n"
        "  |> last()\n"
        "  |> rename(columns: {_value: \"last_price\"})\n"
        "\n"
        "join(tables: {first: first_prices, last: last_prices}, on: [\"ticker\"])\n"
        "  |> map(fn: (r) => ({\n"
        "      ticker: r.ticker,\n"
        "      change_pct: (r.last_price - r.first_price) / r.first_price * 100.0\n"
        "  }))\n"
        "  |> sort(columns: [\"change_pct\"], desc: true)\n"
        "  |> limit(n: " + std::to_string(top_n) + ")";

    const std::string response = execute_query(query);
    if (response.empty()) {
        return nlohmann::json::array();
    }

    return parse_query_response(response);
}

nlohmann::json QueryHandler::get_market_summary() {
    // Flux query — latest price for every ticker in the last hour
    const std::string query =
        "from(bucket: \"" + bucket_ + "\")\n"
        "  |> range(start: -1h)\n"
        "  |> filter(fn: (r) => r._measurement == \"stock_ticks\")\n"
        "  |> filter(fn: (r) => r._field == \"price\")\n"
        "  |> last()\n"
        "  |> group(columns: [\"ticker\"])\n"
        "  |> sort(columns: [\"ticker\"])";

    const std::string response = execute_query(query);
    if (response.empty()) {
        return nlohmann::json::array();
    }

    return parse_query_response(response);
}

std::string QueryHandler::execute_query(const std::string& flux_query) {
    try {
        // influxdb-cxx query returns a vector of Points
        auto points = db_->query(flux_query);

        // Convert points to a simple JSON-serialisable structure
        nlohmann::json result = nlohmann::json::array();

        for (const auto& point : points) {
            // Each point's tags and fields are serialised individually
            result.push_back(nlohmann::json{
                { "name",   point.getName() },
                { "tags",   point.getTags() },
                { "fields", point.getFields() }
            });
        }

        return result.dump();

    } catch (const std::exception& e) {
        spdlog::error("QueryHandler: query failed: {}", e.what());
        return {};
    }
}

nlohmann::json QueryHandler::parse_query_response(const std::string& response) {
    // The response from execute_query() is already JSON — parse and return it
    try {
        return nlohmann::json::parse(response);
    } catch (const nlohmann::json::parse_error& e) {
        spdlog::error("QueryHandler: failed to parse query response: {}", e.what());
        return nlohmann::json::array();
    }
}