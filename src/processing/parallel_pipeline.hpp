#pragma once
#include "thread_pool.hpp"
#include "indicators/moving_average.hpp"
#include "indicators/rsi_calculator.hpp"
#include "indicators/vwap_calculator.hpp"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

// ParallelPipeline extends the stream processor's capabilities by running
// multiple analysis passes over a batch of ticks in parallel. Where
// StreamProcessor handles one tick at a time, ParallelPipeline takes a
// full batch and distributes the work across all available CPU cores.
// Use this for:
//   - End of minute/hour batch aggregations
//   - Running multiple indicator configurations simultaneously
//   - Cross-ticker correlation analysis
//   - Backtesting indicator parameters across historical data

// Data flow:
//   Batch of ticks (multiple tickers)
//       ↓
//   Partition by ticker symbol
//       ↓
//   Submit each partition to ThreadPool  ← runs concurrently
//       ↓
//   Collect futures and wait for results
//       ↓
//   Merge results into single output map

class ParallelPipeline {
public:
    // Result of processing one ticker's batch of ticks
    struct TickerResult {
        std::string ticker;           // ticker symbol
        double last_price{ 0.0 };    // most recent price in the batch
        double sma_20{ 0.0 };        // SMA-20 after processing all ticks
        double sma_50{ 0.0 };        // SMA-50 after processing all ticks
        double rsi_14{ 0.0 };        // RSI-14 after processing all ticks
        double vwap{ 0.0 };          // VWAP across the entire batch
        size_t tick_count{ 0 };      // number of ticks processed
        bool   indicators_ready{ false }; // true once windows are filled
    };

    // Map of ticker symbol → processing result
    using ResultMap = std::unordered_map<std::string, TickerResult>;

    // Callback invoked when a batch finishes processing.
    // Receives the full result map for all tickers in the batch.
    using BatchCallback = std::function<void(const ResultMap& results)>;

    // Constructs the pipeline with a shared thread pool.
    // The pool is shared so the stream processor and pipeline
    // draw from the same worker budget rather than competing.
    explicit ParallelPipeline(std::shared_ptr<ThreadPool> pool);

    // Processes a batch of ticks in parallel, partitioned by ticker symbol.
    // Blocks until all tickers in the batch have been processed.
    // Returns a result map with one entry per unique ticker in the batch.
    //
    // ticks — vector of raw tick JSON objects, each must have a "ticker" field
    ResultMap process_batch(const std::vector<nlohmann::json>& ticks);

    // Async version of process_batch — submits the batch and returns
    // immediately. The callback is invoked on a worker thread when done.
    void process_batch_async(const std::vector<nlohmann::json>& ticks,
                             BatchCallback callback);

    // Returns the total number of batches processed since construction.
    uint64_t batches_processed() const { return batches_processed_.load(); }

    // Returns the total number of ticks processed across all batches.
    uint64_t ticks_processed() const { return ticks_processed_.load(); }

private:
    // Processes all ticks for a single ticker symbol on a worker thread.
    // Returns a TickerResult with the final indicator values.
    TickerResult process_ticker_batch(const std::string& ticker,
                                      const std::vector<nlohmann::json>& ticks);

    // Partitions a flat batch of ticks into per-ticker groups.
    // Returns a map of ticker → vector of that ticker's ticks.
    std::unordered_map<std::string, std::vector<nlohmann::json>>
    partition_by_ticker(const std::vector<nlohmann::json>& ticks);

    std::shared_ptr<ThreadPool> pool_;               // shared worker pool
    std::atomic<uint64_t> batches_processed_{ 0 };   // diagnostic counter
    std::atomic<uint64_t> ticks_processed_{ 0 };     // diagnostic counter
};