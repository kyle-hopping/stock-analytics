#include "kafka_consumer.hpp"
#include <stdexcept>

KafkaConsumer::KafkaConsumer(const std::string& broker, const std::string& group_id)
    : broker_(broker), group_id_(group_id) {

    std::string error_str;

    std::unique_ptr<RdKafka::Conf> conf(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)
    );

    // Set the broker address — can be comma-separated for a cluster
    if (conf->set("bootstrap.servers", broker, error_str) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("KafkaConsumer: failed to set broker: " + error_str);
    }

    // Consumer group ID — consumers in the same group share partitions so
    // work is distributed across multiple instances automatically
    if (conf->set("group.id", group_id, error_str) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("KafkaConsumer: failed to set group.id: " + error_str);
    }

    // Start consuming from the earliest available message if no committed
    // offset exists for this group — ensures no ticks are missed on first run
    if (conf->set("auto.offset.reset", "earliest", error_str) != RdKafka::Conf::CONF_OK) {
        spdlog::warn("KafkaConsumer: could not set auto.offset.reset: {}", error_str);
    }

    // Automatically commit offsets every 5 seconds so that if the consumer
    // restarts it resumes from where it left off rather than replaying everything
    if (conf->set("enable.auto.commit", "true", error_str) != RdKafka::Conf::CONF_OK) {
        spdlog::warn("KafkaConsumer: could not set enable.auto.commit: {}", error_str);
    }

    if (conf->set("auto.commit.interval.ms", "5000", error_str) != RdKafka::Conf::CONF_OK) {
        spdlog::warn("KafkaConsumer: could not set auto.commit.interval.ms: {}", error_str);
    }

    // Register the rebalance callback so we get notified when partitions
    // are assigned or revoked due to consumer group changes
    if (conf->set("rebalance_cb", &rebalance_cb_, error_str) != RdKafka::Conf::CONF_OK) {
        spdlog::warn("KafkaConsumer: could not set rebalance callback: {}", error_str);
    }

    // Create the consumer instance using the configured properties
    consumer_.reset(RdKafka::KafkaConsumer::create(conf.get(), error_str));
    if (!consumer_) {
        throw std::runtime_error("KafkaConsumer: failed to create consumer: " + error_str);
    }

    spdlog::info("KafkaConsumer: connected to broker {} with group.id '{}'",
        broker, group_id);
}

KafkaConsumer::~KafkaConsumer() {
    // Signal the poll loop to stop if it is still running
    stop();

    if (consumer_) {
        // Unsubscribe from all topics and commit final offsets before closing
        consumer_->unsubscribe();
        consumer_->close();
        spdlog::info("KafkaConsumer: closed cleanly");
    }
}

void KafkaConsumer::subscribe(const std::vector<std::string>& topics) {
    if (!consumer_) {
        throw std::runtime_error("KafkaConsumer: cannot subscribe — consumer not initialized");
    }

    RdKafka::ErrorCode err = consumer_->subscribe(topics);
    if (err != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error("KafkaConsumer: subscription failed: " +
            RdKafka::err2str(err));
    }

    // Log all subscribed topics for visibility
    for (const auto& topic : topics) {
        spdlog::info("KafkaConsumer: subscribed to topic '{}'", topic);
    }
}

void KafkaConsumer::start() {
    if (!callback_) {
        throw std::runtime_error("KafkaConsumer: cannot start — no callback registered. "
            "Call set_callback() before start()");
    }

    if (!consumer_) {
        throw std::runtime_error("KafkaConsumer: cannot start — consumer not initialized");
    }

    running_.store(true);
    spdlog::info("KafkaConsumer: poll loop started");

    // Main poll loop - Continuously polls Kafka for new messages. Each call to 
    // consume() blocks for up to POLL_TIMEOUT_MS milliseconds waiting for a
    // message, then returns either a message or a timeout/error.
    while (running_.load()) {
        std::unique_ptr<RdKafka::Message> message(
            consumer_->consume(POLL_TIMEOUT_MS)
        );

        switch (message->err()) {
            case RdKafka::ERR_NO_ERROR:
                // Valid message received — process it and dispatch to the cb
                handle_message(*message);
                break;

            case RdKafka::ERR__TIMED_OUT:
                // No message arrived within the poll timeout — loop and try again.
                break;

            case RdKafka::ERR__PARTITION_EOF:
                // Reached the end of a partition. Normal during low-activity
                // Continue polling.
                spdlog::debug("KafkaConsumer: reached end of partition");
                break;

            default:
                // Unexpected error — log it but keep the loop running so we
                // don't kill the consumer permanently
                spdlog::error("KafkaConsumer: consume error: {}", message->errstr());
                break;
        }
    }

    spdlog::info("KafkaConsumer: poll loop stopped");
}

void KafkaConsumer::handle_message(RdKafka::Message& message) {
    // Extract the message key — this is the ticker symbol set by the producer
    const std::string ticker = message.key() ? *message.key() : "unknown";

    // Extract the raw payload as a string
    const std::string raw_payload(
        static_cast<const char*>(message.payload()),
        message.len()
    );

    // Parse the JSON payload — skip malformed messages rather than crashing
    nlohmann::json payload;
    try {
        payload = nlohmann::json::parse(raw_payload);
    } catch (const nlohmann::json::parse_error& e) {
        spdlog::warn("KafkaConsumer: skipping malformed message for '{}': {}",
            ticker, e.what());
        return;
    }

    spdlog::debug("KafkaConsumer: received tick for {} from partition {} offset {}",
        ticker, message.partition(), message.offset());

    // Dispatch the parsed message to the user-supplied callback for processing
    callback_(ticker, payload);
}

// Rebalance callback 

void KafkaConsumer::RebalanceCb::rebalance_cb(
    RdKafka::KafkaConsumer* consumer,
    RdKafka::ErrorCode err,
    std::vector<RdKafka::TopicPartition*>& partitions) {

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
        // New partitions have been assigned to this consumer instance —
        // log each one and start consuming from them
        spdlog::info("KafkaConsumer: {} partition(s) assigned", partitions.size());
        for (const auto* p : partitions) {
            spdlog::debug("KafkaConsumer:   topic '{}' partition {}",
                p->topic(), p->partition());
        }
        consumer->assign(partitions);
    } else if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
        // Partitions have been revoked — another consumer in the group is
        // taking over. Commit offsets and release the partitions.
        spdlog::info("KafkaConsumer: {} partition(s) revoked", partitions.size());
        consumer->unassign();
    } else {
        // Unexpected rebalance error — log it and unassign for safety!
        spdlog::error("KafkaConsumer: rebalance error: {}", RdKafka::err2str(err));
        consumer->unassign();
    }
}