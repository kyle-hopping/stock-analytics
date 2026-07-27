#include "influxdb_writer.hpp"
#include <InfluxDB/InfluxDBFactory.h>
#include <stdexcept>
#include <chrono>

InfluxDBWriter::InfluxDBWriter() {
    // Load connection parameters from env
    const char* url_env    = std::getenv("INFLUXDB_URL");
    const char* token_env  = std::getenv("INFLUXDB_TOKEN");
    const char* org_env    = std::getenv("INFLUXDB_ORG");
    const char* bucket_env = std::getenv("INFLUXDB_BUCKET");

    if (!url_env || !token_env || !org_env || !bucket_env) {
        throw std::runtime_error(
            "InfluxDBWriter: missing required environment variables. "
            "Ensure INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, "
            "and INFLUXDB_BUCKET are set in your .env file."
        );
    }

    connect(url_env, token_env, org_env, bucket_env);
}

InfluxDBWriter::InfluxDBWriter(const std::string& url, const std::string& token,
                               const std::string& org, const std::string& bucket) {
    connect(url, token, org, bucket);
}

InfluxDBWriter::~InfluxDBWriter() {
    // Flush any remaining buffered points before the connection closes
    // to prevent data loss on clean shutdown
    try {
        flush();
    } catch (const std::exception& e) {
        spdlog::error("InfluxDBWriter: flush on destruction failed: {}", e.what());
    }
}

void InfluxDBWriter::connect(const std::string& url, const std::string& token,
                             const std::string& org, const std::string& bucket) {
    bucket_ = bucket;

    try {
        // influxdb-cxx 0.8.1 uses InfluxDB v1.x compatibility API format.
        // Token is passed as username in the URL, db= is the bucket name.
        // Format: http://token@host:port?db=bucket
        db_ = influxdb::InfluxDBBuilder::http(url + "?db=" + bucket)
            .setAuthToken(token)
            .connect();

        db_->batchOf(BATCH_SIZE);
        spdlog::info("InfluxDBWriter: connected to {} bucket '{}'", url, bucket);
    } catch (const std::exception& e) {
        throw std::runtime_error(
            "InfluxDBWriter: failed to connect to InfluxDB: " +
            std::string(e.what())
        );
    }
}

bool InfluxDBWriter::write_tick(const nlohmann::json& tick) {
    if (!db_) {
        spdlog::error("InfluxDBWriter: cannot write — not connected");
        return false;
    }

    // Validate the tick has the minimum required fields before building a point
    if (!tick.contains("ticker") || !tick.contains("price")) {
        spdlog::warn("InfluxDBWriter: skipping tick missing 'ticker' or 'price' field");
        return false;
    }

    try {
        influxdb::Point point = build_point(tick);

        {
            std::unique_lock<std::mutex> lock(buffer_mutex_);
            write_buffer_.push_back(std::move(point));
        }

        // Flush to InfluxDB if the buffer has reached the batch threshold
        flush_if_needed();

        points_written_.fetch_add(1, std::memory_order_relaxed);
        return true;

    } catch (const std::exception& e) {
        spdlog::error("InfluxDBWriter: failed to write tick for '{}': {}",
            tick.value("ticker", "unknown"), e.what());
        return false;
    }
}

size_t InfluxDBWriter::write_batch(const std::vector<nlohmann::json>& ticks) {
    size_t written = 0;

    for (const auto& tick : ticks) {
        if (write_tick(tick)) {
            ++written;
        }
    }

    // Force a flush after a batch write to ensure all points are persisted
    flush();

    spdlog::info("InfluxDBWriter: batch write complete — {}/{} points written",
        written, ticks.size());

    return written;
}

void InfluxDBWriter::flush() {
    if (!db_) {
        return;
    }

    std::unique_lock<std::mutex> lock(buffer_mutex_);

    if (write_buffer_.empty()) {
        return;
    }

    try {
        // Write all buffered points to InfluxDB in a single HTTP request
        for (auto& point : write_buffer_) {
            db_->write(std::move(point));
        }

        // Flush influxdb-cxx's internal batch buffer to the server
        db_->flushBatch();

        spdlog::debug("InfluxDBWriter: flushed {} points to bucket '{}'",
            write_buffer_.size(), bucket_);

        write_buffer_.clear();

    } catch (const std::exception& e) {
        spdlog::error("InfluxDBWriter: flush failed: {}", e.what());
    }
}

influxdb::Point InfluxDBWriter::build_point(const nlohmann::json& tick) {
    const std::string ticker = tick["ticker"].get<std::string>();
    const double price = tick["price"].get<double>();
    const std::string source = tick.value("source", "unknown");

    // Build the InfluxDB point
    // Tags are indexed strings used for filtering and grouping queries.
    // Fields are the actual numeric/string values being stored.
    auto point = influxdb::Point{ "stock_ticks" }
        // Tags — indexed for fast querying by ticker and data source
        .addTag("ticker", ticker)
        .addTag("source", source)

        // Core price fields
        .addField("price",  price)
        .addField("open",   tick.value("open",   0.0))
        .addField("high",   tick.value("high",   0.0))
        .addField("low",    tick.value("low",    0.0))
        .addField("volume", tick.value("volume", int64_t(0)));

    // Indicator fields — only written if present and indicators are ready.
    // Writing NaN or zero indicator values before the window fills would
    // pollute the time-series data with meaningless early values.
    if (tick.contains("indicators") && tick.value("indicators_ready", false)) {
        const auto& ind = tick["indicators"];

        point.addField("sma_20", ind.value("sma_20", 0.0))
             .addField("sma_50", ind.value("sma_50", 0.0))
             .addField("rsi_14", ind.value("rsi_14", 0.0))
             .addField("vwap",   ind.value("vwap",   0.0));
    }

    return point;
}

void InfluxDBWriter::flush_if_needed() {
    size_t current_size;
    {
        std::unique_lock<std::mutex> lock(buffer_mutex_);
        current_size = write_buffer_.size();
    }

    // Flush without holding the lock — flush() acquires its own lock
    if (current_size >= BATCH_SIZE) {
        flush();
    }
}

size_t InfluxDBWriter::buffer_size() const {
    std::unique_lock<std::mutex> lock(buffer_mutex_);
    return write_buffer_.size();
}