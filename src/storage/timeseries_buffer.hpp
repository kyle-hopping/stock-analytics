#pragma once
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <mutex>
#include <vector>
#include <optional>
#include <string>

// TimeSeriesBuffer is a thread-safe ring buffer that sits between the stream
// processor and the InfluxDB writer. It absorbs bursts of incoming ticks
// when the writer is busy, preventing backpressure from slowing down the
// Kafka consumer. When the buffer is full, oldest entries are overwritten —
// this is intentional since recent data is always more valuable than old.

// Architecture:
//   StreamProcessor → TimeSeriesBuffer → InfluxDBWriter
//                         ↑
//                   absorbs write bursts
//                   decouples producer speed from writer speed
class TimeSeriesBuffer {
public:
    // Constructs the ring buffer with the given capacity.
    // capacity — maximum number of tick entries to hold before overwriting.
    // Throws std::invalid_argument if capacity is zero.
    explicit TimeSeriesBuffer(size_t capacity = 10000);

    // Pushes a new tick into the buffer. If the buffer is full, the oldest
    // entry is overwritten. Thread-safe — safe to call from multiple threads.
    void push(const nlohmann::json& tick);

    // Drains up to max_items ticks from the buffer into the output vector.
    // Returns the number of items drained. Thread-safe.
    size_t drain(std::vector<nlohmann::json>& output, size_t max_items = 500);

    // Returns true if the buffer currently has no items.
    bool empty() const;

    // Returns the number of items currently in the buffer.
    size_t size() const;

    // Returns the maximum number of items the buffer can hold.
    size_t capacity() const { return capacity_; }

    // Returns the total number of ticks pushed since construction.
    uint64_t total_pushed() const { return total_pushed_.load(); }

    // Returns the number of ticks dropped due to buffer overflow.
    // A non-zero value means the writer is not keeping up with the producer.
    uint64_t total_overwritten() const { return total_overwritten_.load(); }

    // Clears all items from the buffer.
    void clear();

private:
    size_t capacity_;                       // maximum buffer capacity
    std::vector<nlohmann::json> buffer_;    // underlying ring storage
    size_t head_{ 0 };                      // index of next write position
    size_t tail_{ 0 };                      // index of next read position
    size_t count_{ 0 };                     // current number of items
    mutable std::mutex mutex_;              // protects all buffer state

    std::atomic<uint64_t> total_pushed_{ 0 };       // diagnostic counter
    std::atomic<uint64_t> total_overwritten_{ 0 };  // overflow counter
};