#include "timeseries_buffer.hpp"
#include <stdexcept>

TimeSeriesBuffer::TimeSeriesBuffer(size_t capacity) : capacity_(capacity) {
    if (capacity == 0) {
        throw std::invalid_argument("TimeSeriesBuffer: capacity must be greater than zero");
    }

    // Pre-allocate the full buffer up front to avoid reallocations
    // during operation — predictable memory usage is important for
    // 24/7 pipeline
    buffer_.resize(capacity);

    spdlog::info("TimeSeriesBuffer: initialised with capacity {}", capacity);
}

void TimeSeriesBuffer::push(const nlohmann::json& tick) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (count_ == capacity_) {
        // Buffer is full — overwrite the oldest entry by advancing the tail.
        // We log a warning periodically but don't block — a slow writer
        // should never stall the Kafka consumer.
        tail_ = (tail_ + 1) % capacity_;
        total_overwritten_.fetch_add(1, std::memory_order_relaxed);

        // Log a warning every 1000 overwrites to flag sustained backpressure
        if (total_overwritten_.load() % 1000 == 0) {
            spdlog::warn("TimeSeriesBuffer: {} ticks overwritten — "
                "InfluxDB writer may not be keeping up with tick velocity",
                total_overwritten_.load());
        }
    } else {
        ++count_;
    }

    // Write the new tick at the current head position and advance
    buffer_[head_] = tick;
    head_ = (head_ + 1) % capacity_;

    total_pushed_.fetch_add(1, std::memory_order_relaxed);
}

size_t TimeSeriesBuffer::drain(std::vector<nlohmann::json>& output, size_t max_items) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (count_ == 0) {
        return 0;
    }

    // Drain up to max_items from the tail of the buffer — oldest first
    const size_t to_drain = std::min(max_items, count_);

    output.reserve(output.size() + to_drain);

    for (size_t i = 0; i < to_drain; ++i) {
        output.push_back(std::move(buffer_[tail_]));
        tail_ = (tail_ + 1) % capacity_;
    }

    count_ -= to_drain;

    spdlog::debug("TimeSeriesBuffer: drained {} items, {} remaining",
        to_drain, count_);

    return to_drain;
}

bool TimeSeriesBuffer::empty() {
    std::unique_lock<std::mutex> lock(mutex_);
    return count_ == 0;
}

size_t TimeSeriesBuffer::size() {
    std::unique_lock<std::mutex> lock(mutex_);
    return count_;
}

void TimeSeriesBuffer::clear() {
    std::unique_lock<std::mutex> lock(mutex_);
    head_  = 0;
    tail_  = 0;
    count_ = 0;
    spdlog::info("TimeSeriesBuffer: cleared");
}