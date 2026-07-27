#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <nlohmann/json.hpp>
#include <atomic>
#include <chrono>
#include <csignal>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "ingestion/stock_fetcher.hpp"
#include "ingestion/kafka_producer.hpp"
#include "ingestion/kafka_consumer.hpp"
#include "processing/thread_pool.hpp"
#include "processing/stream_processor.hpp"
#include "processing/parallel_pipeline.hpp"
#include "storage/influxdb_writer.hpp"
#include "storage/timeseries_buffer.hpp"
#include "dashboard/websocket_server.hpp"
#include "dashboard/query_handler.hpp"

// Global shutdown flag
// Set to true by the signal handler when SIGINT or SIGTERM is received.
// All subsystems check this flag and shut down gracefully when it is set.
static std::atomic<bool> g_shutdown{ false };

// Signal handler
// Catches Ctrl+C (SIGINT) and kill signals (SIGTERM) so the pipeline can
// flush buffers, close Kafka connections, and drain InfluxDB writes cleanly
// before the process exits.
static void signal_handler(int signal) {
    spdlog::warn("main: received signal {} — initiating graceful shutdown...", signal);
    g_shutdown.store(true);
}

// Env variable loader
// Reads a required environment variable and throws if it is not set.
// All API keys and config values come from .env — never hardcoded.
static std::string require_env(const std::string& name) {
    const char* value = std::getenv(name.c_str());
    if (!value || std::string(value).empty()) {
        throw std::runtime_error(
            "main: required environment variable '" + name + "' is not set. "
            "Check your .env file."
        );
    }
    return value;
}

// Optional env variable loader
// Returns the environment variable value or a default if not set.
static std::string optional_env(const std::string& name,
                                const std::string& default_value) {
    const char* value = std::getenv(name.c_str());
    return (value && !std::string(value).empty()) ? value : default_value;
}

// Logger setup
// Configures spdlog to write to both the console (coloured) and a log file.
// Log level is set to INFO by default — change to DEBUG for verbose output.
static void setup_logging() {
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_level(spdlog::level::info);

    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(
        "stock_analytics.log", true // truncate log on each run
    );
    file_sink->set_level(spdlog::level::debug);

    auto logger = std::make_shared<spdlog::logger>(
        "stock_analytics",
        spdlog::sinks_init_list{ console_sink, file_sink }
    );

    logger->set_level(spdlog::level::debug);
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");

    spdlog::set_default_logger(logger);
    spdlog::info("main: logging initialised — writing to stock_analytics.log");
}

// Startup banner
static void print_banner() {
    spdlog::info("╔═══════════════════════════════════════════╗");
    spdlog::info("║         Stock Analytics Dashboard         ║");
    spdlog::info("╚═══════════════════════════════════════════╝");
}

int main() {
    // Startup logging
    setup_logging();
    print_banner();

    // Signal handling
    // Register handlers for clean shutdown on Ctrl+C and kill signals
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    try {
        // Load config from environment
        // All config comes from .env — see .env.example for required variables
        const std::string kafka_broker = require_env("KAFKA_BROKER");
        const std::string kafka_topic_raw = require_env("KAFKA_TOPIC_RAW");
        const std::string kafka_topic_processed = require_env("KAFKA_TOPIC_PROCESSED");
        const int ws_port = std::stoi(
            optional_env("WEBSOCKET_PORT", "9001")
        );

        // Tickers to watch — comma-separated list or hardcoded defaults
        const std::vector<std::string> tickers = {
            "AAPL", "TSLA", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "AMD",
            "NFLX", "SHOP"
        };

        spdlog::info("main: configuration loaded");
        spdlog::info("main:   kafka broker    = {}", kafka_broker);
        spdlog::info("main:   raw topic       = {}", kafka_topic_raw);
        spdlog::info("main:   processed topic = {}", kafka_topic_processed);
        spdlog::info("main:   websocket port  = {}", ws_port);
        spdlog::info("main:   watching {} tickers", tickers.size());

        // Initialise subsystems
        spdlog::info("main: initialising subsystems...");

        // 1. Thread pool — shared across all parallel workloads.
        // Size defaults to hardware concurrency (number of logical CPU cores).
        auto thread_pool = std::make_shared<ThreadPool>();
        spdlog::info("main: thread pool started with {} workers", thread_pool->size());

        // 2. InfluxDB writer — persists enriched ticks to time-series storage.
        // Reads connection details from INFLUXDB_* environment variables.
        auto influx_writer = std::make_shared<InfluxDBWriter>();
        spdlog::info("main: InfluxDB writer connected");

        // 3. Time-series buffer — decouples tick production from InfluxDB writes.
        // Absorbs write bursts so a slow InfluxDB never stalls the pipeline.
        auto ts_buffer = std::make_shared<TimeSeriesBuffer>(10000);
        spdlog::info("main: time-series buffer initialised (capacity=10000)");

        // 4. Stream processor — consumes raw_ticks, calculates indicators,
        // publishes enriched ticks to processed_ticks. Runs the Kafka
        // consumer poll loop on its own thread.
        auto stream_processor = std::make_shared<StreamProcessor>(
            kafka_broker,
            kafka_topic_raw,
            kafka_topic_processed,
            thread_pool->size()
        );
        spdlog::info("main: stream processor initialised");

        // 5. Parallel pipeline — handles batch aggregation workloads.
        // Shares the thread pool with the stream processor.
        auto parallel_pipeline = std::make_shared<ParallelPipeline>(thread_pool);
        spdlog::info("main: parallel pipeline initialised");

        // 6. WebSocket server — pushes enriched ticks to browser clients.
        // Runs on a dedicated thread, non-blocking.
        auto ws_server = std::make_shared<WebSocketServer>(ws_port);
        ws_server->start();
        spdlog::info("main: WebSocket server started on port {}", ws_port);

        // 7. Query handler — reads historical data from InfluxDB for the dashboard.
        auto query_handler = std::make_shared<QueryHandler>();
        spdlog::info("main: query handler connected");

        // 8. Stock fetcher — polls Alpha Vantage and Massive for live tick data.
        // Publishes each tick directly to the raw_ticks Kafka topic.
        auto stock_fetcher = std::make_shared<StockFetcher>();
        for (const auto& ticker : tickers) {
            stock_fetcher->add_ticker(ticker);
        }

        // Wire the fetcher's callback to a KafkaProducer so every tick
        // received from the APIs is immediately published to Kafka
        auto raw_producer = std::make_shared<KafkaProducer>(
            kafka_broker, kafka_topic_raw
        );
        stock_fetcher->set_callback([&raw_producer](const std::string& ticker,
                                                     const nlohmann::json& tick) {
            raw_producer->publish(ticker, tick);
        });
        spdlog::info("main: stock fetcher initialised for {} tickers", tickers.size());

        // Wire processed ticks to InfluxDB and WebSocket
        // Subscribe a second Kafka consumer to the processed_ticks topic.
        // Every enriched tick it receives is:
        //      a) pushed into the time-series buffer for batched InfluxDB writes
        //      b) broadcast to all connected WebSocket dashboard clients
        auto processed_consumer = std::make_shared<KafkaConsumer>(
            kafka_broker, "influxdb_writer_group"
        );
        processed_consumer->subscribe({ kafka_topic_processed });
        processed_consumer->set_callback([&ts_buffer, &ws_server](
            const std::string& ticker,
            const nlohmann::json& tick) {

            // Push to the ring buffer — the drain loop below writes to InfluxDB
            ts_buffer->push(tick);

            // Immediately broadcast to WebSocket clients — low latency path
            ws_server->broadcast_tick(tick);
        });

        spdlog::info("main: all subsystems initialised — starting pipeline");
        spdlog::info("main: press Ctrl+C to shut down gracefully");

        // Launch background threads

        // Thread A — Stock fetcher polling loop
        // Polls Alpha Vantage and Massive on a 60-second interval
        std::thread fetcher_thread([&stock_fetcher, &g_shutdown]() {
            spdlog::info("fetcher_thread: starting poll loop");
            stock_fetcher->start_polling(std::chrono::seconds(60));
            spdlog::info("fetcher_thread: stopped");
        });

        // Thread B — Stream processor consumer loop
        // Runs the Kafka consumer for raw_ticks, fans out to thread pool
        std::thread processor_thread([&stream_processor]() {
            spdlog::info("processor_thread: starting consumer loop");
            stream_processor->start();
            spdlog::info("processor_thread: stopped");
        });

        // Thread C — Processed tick consumer loop
        // Drains processed_ticks into InfluxDB and WebSocket
        std::thread consumer_thread([&processed_consumer]() {
            spdlog::info("consumer_thread: starting processed tick consumer");
            processed_consumer->start();
            spdlog::info("consumer_thread: stopped");
        });

        // Main loop — InfluxDB drain
        // The main thread drains the time-series buffer into InfluxDB every
        // 500ms. This decouples InfluxDB write latency from tick processing
        // and allows batched writes for better throughput.
        spdlog::info("main: entering InfluxDB drain loop");

        while (!g_shutdown.load()) {
            std::vector<nlohmann::json> batch;

            // Drain up to 500 ticks from the buffer per cycle
            const size_t drained = ts_buffer->drain(batch, 500);

            if (drained > 0) {
                // Write the drained batch to InfluxDB in one operation
                const size_t written = influx_writer->write_batch(batch);
                spdlog::debug("main: wrote {}/{} ticks to InfluxDB", written, drained);
            }

            // Log pipeline health every 30 seconds
            static auto last_health_log = std::chrono::steady_clock::now();
            const auto  now = std::chrono::steady_clock::now();

            if (now - last_health_log >= std::chrono::seconds(30)) {
                spdlog::info("main: ── Pipeline Health ──────────────────");
                spdlog::info("main: stream processor ticks: {}",
                    stream_processor->ticks_processed());
                spdlog::info("main: influx points written: {}",
                    influx_writer->points_written());
                spdlog::info("main: buffer size: {}",
                    ts_buffer->size());
                spdlog::info("main: buffer overwrites: {}",
                    ts_buffer->total_overwritten());
                spdlog::info("main: ws clients connected: {}",
                    ws_server->client_count());
                spdlog::info("main: ─────────────────────────────────────");
                last_health_log = now;
            }

            // Sleep 500ms between drain cycles — short enough to keep
            // InfluxDB writes timely, long enough not to spin the CPU
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        // Graceful shutdown
        spdlog::info("main: shutdown signal received — stopping all subsystems...");

        // Stop all subsystems in reverse dependency order
        stock_fetcher->stop();
        stream_processor->stop();
        processed_consumer->stop();
        ws_server->stop();

        // Wait for all threads to finish their current work and exit
        if (fetcher_thread.joinable())   fetcher_thread.join();
        if (processor_thread.joinable()) processor_thread.join();
        if (consumer_thread.joinable())  consumer_thread.join();

        // Final InfluxDB drain — flush any remaining buffered ticks
        spdlog::info("main: performing final InfluxDB drain...");
        std::vector<nlohmann::json> final_batch;
        ts_buffer->drain(final_batch, ts_buffer->size());
        if (!final_batch.empty()) {
            influx_writer->write_batch(final_batch);
        }

        // Flush the InfluxDB write buffer completely
        influx_writer->flush();

        spdlog::info("main: ── Shutdown Complete ───────────────────────");
        spdlog::info("main: total ticks processed: {}",
            stream_processor->ticks_processed());
        spdlog::info("main: total points written: {}",
            influx_writer->points_written());
        spdlog::info("main: total overwrites: {}",
            ts_buffer->total_overwritten());
        spdlog::info("main: ────────────────────────────────────────────");

    } catch (const std::exception& e) {
        spdlog::critical("main: FATAL error — {}", e.what());
        return 1;
    }

    spdlog::info("main: goodbye");
    return 0;
}