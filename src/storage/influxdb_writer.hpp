#pragma once
#include <InfluxDBFactory.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// InfluxDBWriter handles all writes to the InfluxDB time-series database.
// It receives enriched tick data from the stream processor and persists it
// using InfluxDB's line protocol. Writes are batched for throughput and
// flushed either when the batch reaches a size threshold or on a timer.

// InfluxDB concepts used here:
//   Measurement — the table name e.g. "stock_ticks"
//   Tags        — indexed metadata fields e.g. ticker, source
//   Fields      — numeric/string values e.g. price, rsi, volume
//   Timestamp   — nanosecond precision Unix timestamp

// Example line protocol output:
//   stock_ticks,ticker=AAPL,source=alpha_vantage
//     price=182.63,sma_20=181.45,rsi_14=62.3,vwap=182.10,volume=52341200
//     1705329000000000000

class InfluxDBWriter {
public:
    // Constructs the writer and connects to InfluxDB using environment variables:
    //   INFLUXDB_URL    — e.g. "http://localhost:8086"
    //   INFLUXDB_TOKEN  — authentication token
    //   INFLUXDB_ORG    — organisation name
    //   INFLUXDB_BUCKET — bucket to write into
    // Throws std::runtime_error if any required env var is missing or
    // if the connection cannot be established.
    InfluxDBWriter();

    // Constructs the writer with explicit connection parameters.
    // Useful for switching between local Docker and cloud InfluxDB.
    explicit InfluxDBWriter(const std::string& url,
                            const std::string& token,
                            const std::string& org,
                            const std::string& bucket);

    // Destructor flushes any buffered points before closing the connection.
    ~InfluxDBWriter();

    // Writes a single enriched tick to InfluxDB. The tick must contain
    // at minimum a "ticker" field and a "price" field. Indicator fields
    // are written if present. Batches internally — may not write immediately.
    // Returns true if the point was accepted into the write buffer.
    bool write_tick(const nlohmann::json& tick);

    // Writes a batch of enriched ticks in a single operation.
    // More efficient than calling write_tick() in a loop.
    // Returns the number of points successfully written.
    size_t write_batch(const std::vector<nlohmann::json>& ticks);

    // Immediately flushes all buffered points to InfluxDB.
    // Called automatically by the destructor and when the buffer is full.
    void flush();

    // Returns true if the writer is connected and ready to accept writes.
    bool is_connected() const { return db_ != nullptr; }

    // Returns the total number of points written since construction.
    uint64_t points_written() const { return points_written_.load(); }

    // Returns the number of points currently waiting in the write buffer.
    size_t buffer_size() const;

private:
    // Connects to InfluxDB with the given parameters.
    // Called by both constructors.
    void connect(const std::string& url,
                 const std::string& token,
                 const std::string& org,
                 const std::string& bucket);

    // Builds an InfluxDB Point from a tick JSON object.
    // Returns an empty optional if the tick is missing required fields.
    influxdb::Point build_point(const nlohmann::json& tick);

    // Flushes the buffer if it has reached the batch size threshold.
    void flush_if_needed();

    std::unique_ptr<influxdb::InfluxDB> db_;         // InfluxDB client handle
    std::string bucket_;                             // target bucket name
    std::vector<influxdb::Point> write_buffer_;      // pending points
    mutable std::mutex buffer_mutex_;                // protects write_buffer_
    std::atomic<uint64_t> points_written_{ 0 };      // diagnostic counter

    // Flush the write buffer when it reaches this many points.
    // Larger batches are more efficient but increase data loss risk on crash.
    static constexpr size_t BATCH_SIZE = 500;
};