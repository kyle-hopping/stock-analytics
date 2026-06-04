#pragma once
#include "../ingestion/kafka_consumer.hpp"
#include "../ingestion/kafka_producer.hpp"
#include "thread_pool.hpp"
#include "indicators/moving_average.hpp"
#include "indicators/rsi_calculator.hpp"
#include "indicators/vwap_calculator.hpp"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>

// StreamProcessor is the central processing hub of the analytics pipeline.
// It consumes raw stock ticks from Kafka, fans them out to the thread pool
// for parallel indicator calculation, and publishes the enriched results
// back to Kafka on the processed_ticks topic.

// Data flow:
//   Kafka raw_ticks
//      ↓
//   StreamProcessor::on_tick()        ← called by KafkaConsumer callback
//      ↓
//   ThreadPool::submit()              ← fans out per ticker symbol
//      ↓
//   MovingAverage + RSI + VWAP        ← runs in parallel on worker threads
//      ↓
//   KafkaProducer → processed_ticks   ← enriched tick published back to Kafka

class StreamProcessor {
public:
    // Constructs the processor, wiring together the consumer, producer,
    // and thread pool. Initialises per-ticker indicator state.
    // broker       — Kafka broker address e.g. "localhost:9092"
    // raw_topic    — topic to consume raw ticks from e.g. "raw_ticks"
    // output_topic — topic to publish enriched ticks to e.g. "processed_ticks"
    // num_threads  — worker thread count, defaults to hardware concurrency
    StreamProcessor(const std::string& broker,
                    const std::string& raw_topic,
                    const std::string& output_topic,
                    size_t num_threads = std::thread::hardware_concurrency());

    // Destructor stops the consumer and waits for all in-flight tasks to finish.
    ~StreamProcessor();

    // Starts the consumer poll loop — blocks the calling thread until stop()
    // is called from another thread.
    void start();

    // Signals the consumer poll loop to stop. Safe to call from any thread.
    void stop();

    // Returns true if the processor is currently running.
    bool is_running() const { return running_.load(); }

    // Returns the total number of ticks processed since start() was called.
    uint64_t ticks_processed() const { return ticks_processed_.load(); }

private:
    // Callback registered with KafkaConsumer — invoked for every raw tick.
    // Submits a processing task to the thread pool for the given ticker.
    void on_tick(const std::string& ticker, const nlohmann::json& tick);

    // Processes a single tick on a worker thread — calculates all indicators
    // and publishes the enriched result to the processed_ticks topic.
    // This method runs concurrently for different ticker symbols.
    void process_tick(const std::string& ticker, const nlohmann::json& tick);

    // Returns or creates the indicator state for a given ticker symbol.
    // Thread-safe — protected by indicators_mutex_.
    struct TickerIndicators {
        MovingAverage  sma_20;   // 20-period simple moving average
        MovingAverage  sma_50;   // 50-period simple moving average
        RSICalculator  rsi_14;   // 14-period relative strength index
        VWAPCalculator vwap;     // volume-weighted average price

        TickerIndicators()
            : sma_20(20), sma_50(50), rsi_14(14) {}
    };

    // Gets or creates indicator state for a ticker — initialises on first tick.
    TickerIndicators& get_indicators(const std::string& ticker);

    std::unique_ptr<KafkaConsumer> consumer_;   // consumes raw_ticks
    std::unique_ptr<KafkaProducer> producer_;   // publishes processed_ticks
    std::unique_ptr<ThreadPool>    pool_;        // parallel worker pool

    // Per-ticker indicator state — each symbol has its own independent
    // set of indicators so their histories don't interfere with each other
    std::unordered_map<std::string, TickerIndicators> indicators_;
    mutable std::mutex indicators_mutex_;        // protects indicators_ map

    std::atomic<bool>     running_{ false };     // controls the processing loop
    std::atomic<uint64_t> ticks_processed_{ 0 }; // diagnostic counter

    std::string output_topic_; // processed_ticks topic name for logging
};