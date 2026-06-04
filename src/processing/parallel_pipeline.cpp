#include "parallel_pipeline.hpp"
#include <stdexcept>

ParallelPipeline::ParallelPipeline(std::shared_ptr<ThreadPool> pool)
    : pool_(std::move(pool)) {

    if (!pool_) {
        throw std::runtime_error("ParallelPipeline: thread pool cannot be null");
    }

    spdlog::info("ParallelPipeline: initialised with {} worker threads",
        pool_->size());
}

ParallelPipeline::ResultMap ParallelPipeline::process_batch(
    const std::vector<nlohmann::json>& ticks) {

    if (ticks.empty()) {
        spdlog::warn("ParallelPipeline: process_batch called with empty tick batch");
        return {};
    }

    spdlog::debug("ParallelPipeline: processing batch of {} ticks", ticks.size());

    // Partition ticks by ticker symbol
    // Group all ticks for the same symbol together so each worker thread
    // processes one complete ticker history rather than individual ticks.
    // This avoids any shared mutable state between concurrent tasks.
    const auto partitions = partition_by_ticker(ticks);

    spdlog::debug("ParallelPipeline: batch contains {} unique tickers",
        partitions.size());

    // Submit one task per ticker to the thread pool
    // Each ticker's partition is processed independently and concurrently.
    // We collect futures so we can wait for all of them to finish.
    std::vector<std::future<TickerResult>> futures;
    futures.reserve(partitions.size());

    for (const auto& [ticker, ticker_ticks] : partitions) {
        // Capture by value — the lambda must own its data since it runs
        // asynchronously on a worker thread after this loop completes
        futures.push_back(
            pool_->submit([this, ticker, ticker_ticks]() {
                return process_ticker_batch(ticker, ticker_ticks);
            })
        );
    }

    // Collect results from all futures
    // Wait for every ticker's processing task to complete and merge results
    // into a single output map. future::get() blocks if the task is still running.
    ResultMap results;
    results.reserve(partitions.size());

    for (auto& future : futures) {
        try {
            TickerResult result = future.get();
            const std::string ticker = result.ticker;
            results.emplace(ticker, std::move(result));
        } catch (const std::exception& e) {
            // A single ticker failing should not abort the entire batch —
            // log the error and continue collecting the remaining results
            spdlog::error("ParallelPipeline: task failed with exception: {}", e.what());
        }
    }

    // Update diagnostic counters
    batches_processed_.fetch_add(1, std::memory_order_relaxed);
    ticks_processed_.fetch_add(ticks.size(), std::memory_order_relaxed);

    spdlog::info("ParallelPipeline: batch complete — {} tickers processed, "
        "{} batches total", results.size(), batches_processed_.load());

    return results;
}

void ParallelPipeline::process_batch_async(const std::vector<nlohmann::json>& ticks,
                                            BatchCallback callback) {
    // Submit the entire batch processing as a single task to the pool.
    // The callback is invoked on the worker thread when processing completes.
    pool_->submit([this, ticks, callback]() {
        const ResultMap results = process_batch(ticks);
        if (callback) {
            callback(results);
        }
    });
}

ParallelPipeline::TickerResult ParallelPipeline::process_ticker_batch(
    const std::string& ticker,
    const std::vector<nlohmann::json>& ticks) {

    // Each call to this function runs on its own worker thread with its own
    // local indicator instances — completely isolated from other tickers.
    // No mutexes needed here since all state is local to this call.
    MovingAverage  sma_20(20);
    MovingAverage  sma_50(50);
    RSICalculator  rsi_14(14);
    VWAPCalculator vwap;

    TickerResult result;
    result.ticker     = ticker;
    result.tick_count = ticks.size();

    for (const auto& tick : ticks) {
        const double  price  = tick.value("price",  0.0);
        const double  high   = tick.value("high",   0.0);
        const double  low    = tick.value("low",    0.0);
        const int64_t volume = tick.value("volume", int64_t(0));

        // Skip ticks with invalid price data — bad API responses can
        // occasionally return zero or negative prices
        if (price <= 0.0) {
            spdlog::warn("ParallelPipeline: skipping invalid tick for '{}' "
                "price={:.4f}", ticker, price);
            continue;
        }

        // Feed each tick into all indicators in sequence — the order matters
        // since each indicator maintains its own rolling state
        result.last_price = price;
        result.sma_20     = sma_20.update(price);
        result.sma_50     = sma_50.update(price);
        result.rsi_14     = rsi_14.update(price);
        result.vwap       = vwap.update(price, high, low, volume);
    }

    // Mark indicators as ready only when the longest window (SMA-50) is full
    result.indicators_ready = sma_50.is_ready();

    spdlog::debug("ParallelPipeline: {} — {} ticks, price={:.2f} "
        "sma20={:.2f} sma50={:.2f} rsi={:.2f} vwap={:.2f}",
        ticker, ticks.size(), result.last_price,
        result.sma_20, result.sma_50, result.rsi_14, result.vwap);

    return result;
}

std::unordered_map<std::string, std::vector<nlohmann::json>>
ParallelPipeline::partition_by_ticker(const std::vector<nlohmann::json>& ticks) {
    std::unordered_map<std::string, std::vector<nlohmann::json>> partitions;

    for (const auto& tick : ticks) {
        // Skip ticks missing the ticker field — cannot route them correctly
        if (!tick.contains("ticker")) {
            spdlog::warn("ParallelPipeline: skipping tick missing 'ticker' field");
            continue;
        }

        const std::string ticker = tick["ticker"].get<std::string>();
        partitions[ticker].push_back(tick);
    }

    return partitions;
}