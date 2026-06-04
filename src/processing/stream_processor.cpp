#include "stream_processor.hpp"
#include <stdexcept>

StreamProcessor::StreamProcessor(const std::string& broker,
                                 const std::string& raw_topic,
                                 const std::string& output_topic,
                                 size_t num_threads)
    : output_topic_(output_topic) {

    spdlog::info("StreamProcessor: initialising with {} worker threads", num_threads);

    // Init the thread pool
    pool_ = std::make_unique<ThreadPool>(num_threads);

    // Init the Kafka consumer
    // Uses a dedicated consumer group so the stream processor gets all
    // partitions — no load balancing with other consumer instances
    consumer_ = std::make_unique<KafkaConsumer>(broker, "stream_processor_group");
    consumer_->subscribe({ raw_topic });

    // Register our tick handler as the consumer callback — every raw tick
    // received from Kafka will be dispatched here for processing
    consumer_->set_callback([this](const std::string& ticker,
                                   const nlohmann::json& tick) {
        on_tick(ticker, tick);
    });

    // Init the Kafka producer
    // Publishes enriched ticks with calculated indicators back to Kafka
    producer_ = std::make_unique<KafkaProducer>(broker, output_topic);

    spdlog::info("StreamProcessor: ready — consuming '{}' → publishing '{}'",
        raw_topic, output_topic);
}

StreamProcessor::~StreamProcessor() {
    stop();
}

void StreamProcessor::start() {
    if (!pool_ || !consumer_ || !producer_) {
        throw std::runtime_error("StreamProcessor: cannot start — not fully initialised");
    }

    running_.store(true);
    spdlog::info("StreamProcessor: starting — waiting for ticks...");

    // Blocks here until stop() is called — the consumer poll loop runs on
    // this thread while worker threads handle the actual indicator calculations
    consumer_->start();
}

void StreamProcessor::stop() {
    running_.store(false);

    if (consumer_) {
        consumer_->stop();
    }

    spdlog::info("StreamProcessor: stopped — {} ticks processed total",
        ticks_processed_.load());
}

void StreamProcessor::on_tick(const std::string& ticker, const nlohmann::json& tick) {
    // This is called on the consumer's poll thread for every raw tick received.
    // We immediately hand off to the thread pool so the poll thread is never
    // blocked by indicator calculations — keeping Kafka consumption fast.
    pool_->submit([this, ticker, tick]() {
        process_tick(ticker, tick);
    });
}

void StreamProcessor::process_tick(const std::string& ticker, const nlohmann::json& tick) {
    // This runs on a worker thread — potentially concurrently with other tickers.
    // Each ticker has its own indicator state so there is no shared mutable
    // state between concurrent executions for different symbols.
    const double price  = tick.value("price",  0.0);
    const double high   = tick.value("high",   0.0);
    const double low    = tick.value("low",    0.0);
    const int64_t volume = tick.value("volume", 0);

    if (price <= 0.0) {
        spdlog::warn("StreamProcessor: skipping tick for '{}' — invalid price {:.4f}",
            ticker, price);
        return;
    }

    // Calculate indicators
    // get_indicators() is thread-safe — it locks only when creating a new
    // ticker entry. Subsequent calls for existing tickers are lock-free.
    TickerIndicators& ind = get_indicators(ticker);

    // Feed the new price into each indicator — each maintains its own
    // internal rolling window and returns the latest calculated value
    const double sma_20 = ind.sma_20.update(price);
    const double sma_50 = ind.sma_50.update(price);
    const double rsi_14 = ind.rsi_14.update(price);
    const double vwap   = ind.vwap.update(price, high, low, volume);

    // Build tick payload
    // Start with the original raw tick and append the calculated indicators
    nlohmann::json enriched = tick;
    enriched["indicators"] = {
        { "sma_20", sma_20 },
        { "sma_50", sma_50 },
        { "rsi_14", rsi_14 },
        { "vwap",   vwap   }
    };

    // Flag whether indicators have enough data to be meaningful.
    // SMA-50 needs 50 data points before its value is statistically valid.
    enriched["indicators_ready"] = ind.sma_50.is_ready();

    spdlog::debug("StreamProcessor: {} price={:.2f} sma20={:.2f} sma50={:.2f} "
        "rsi={:.2f} vwap={:.2f}",
        ticker, price, sma_20, sma_50, rsi_14, vwap);

    // Publish tick to processed_ticks topic
    const bool published = producer_->publish(ticker, enriched);
    if (!published) {
        spdlog::error("StreamProcessor: failed to publish enriched tick for '{}'", ticker);
    }

    // Increment the diagnostic counter — useful for monitoring throughput
    ticks_processed_.fetch_add(1, std::memory_order_relaxed);
}

StreamProcessor::TickerIndicators& StreamProcessor::get_indicators(
    const std::string& ticker) {

    // Fast path — ticker already exists, no lock needed after construction.
    // unordered_map reads are safe without a lock if no concurrent writes occur.
    {
        std::unique_lock<std::mutex> lock(indicators_mutex_);
        auto it = indicators_.find(ticker);
        if (it != indicators_.end()) {
            return it->second;
        }

        // First tick for this ticker — create and initialise its indicator state
        spdlog::info("StreamProcessor: initialising indicators for new ticker '{}'", ticker);
        indicators_.emplace(ticker, TickerIndicators{});
        return indicators_.at(ticker);
    }
}