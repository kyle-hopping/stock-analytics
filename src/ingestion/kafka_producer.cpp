#include "kafka_producer.hpp"
#include <stdexcept>

KafkaProducer::KafkaProducer(const std::string& broker, const std::string& topic)
    : broker_(broker), topic_name_(topic) {

    std::string error_str;

    std::unique_ptr<RdKafka::Conf> conf(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)
    );

    // Set the broker address — comma-separated list for a cluster
    if (conf->set("bootstrap.servers", broker, error_str) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("KafkaProducer: failed to set broker: " + error_str);
    }

    // Register the delivery report callback so we get notified when messages
    // are confirmed delivered or permanently failed
    if (conf->set("dr_cb", &delivery_cb_, error_str) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("KafkaProducer: failed to set delivery callback: " + error_str);
    }

    // Batch messages for up to 5ms before sending — improves throughput
    // for high-frequency tick data without adding meaningful latency
    // Testing will be done later on for optimal time
    if (conf->set("linger.ms", "5", error_str) != RdKafka::Conf::CONF_OK) {
        spdlog::warn("KafkaProducer: could not set linger.ms: {}", error_str);
    }

    // Create the producer instance using the configured properties
    producer_.reset(RdKafka::Producer::create(conf.get(), error_str));
    if (!producer_) {
        throw std::runtime_error("KafkaProducer: failed to create producer: " + error_str);
    }

    // Create a topic handle for the target topic, used for all produce calls
    std::unique_ptr<RdKafka::Conf> topic_conf(
        RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)
    );

    // Use consistent hashing so the same ticker symbol always maps to the
    // same partition — this preserves message order per stock symbol
    if (topic_conf->set("partitioner", "consistent_random", error_str) != RdKafka::Conf::CONF_OK) {
        spdlog::warn("KafkaProducer: could not set partitioner: {}", error_str);
    }

    topic_.reset(RdKafka::Topic::create(producer_.get(), topic, topic_conf.get(), error_str));
    if (!topic_) {
        throw std::runtime_error("KafkaProducer: failed to create topic handle: " + error_str);
    }

    spdlog::info("KafkaProducer: connected to broker {} on topic {}", broker, topic);
}

KafkaProducer::~KafkaProducer() {
    if (producer_) {
        spdlog::info("KafkaProducer: flushing outstanding messages before shutdown...");

        // Wait up to 10 seconds for all in-flight messages to be delivered
        // before destroying the producer — prevents message loss on shutdown
        RdKafka::ErrorCode err = producer_->flush(10000);
        if (err != RdKafka::ERR_NO_ERROR) {
            spdlog::warn("KafkaProducer: flush timed out — {} messages may be lost",
                producer_->outq_len());
        }
    }
}

bool KafkaProducer::publish(const std::string& ticker, const nlohmann::json& payload) {
    // Serialise the JSON payload to a UTF-8 string for transport over Kafka
    const std::string message = payload.dump();

    // Produce the message — RD_KAFKA_MSG_F_COPY tells librdkafka to copy the
    // payload buffer so we can safely free it after this call returns
    RdKafka::ErrorCode err = producer_->produce(
        topic_.get(),
        RdKafka::Topic::PARTITION_UA,       // let the partitioner choose
        RdKafka::Producer::RK_MSG_COPY,     // copy payload buffer
        const_cast<char*>(message.c_str()), // message payload
        message.size(),                     // payload size in bytes
        &ticker,                            // message key — ticker symbol
        nullptr                             // optional per-message opaque pointer
    );

    if (err != RdKafka::ERR_NO_ERROR) {
        spdlog::error("KafkaProducer: failed to enqueue message for {}: {}",
            ticker, RdKafka::err2str(err));
        return false;
    }

    // Poll to trigger delivery callbacks and keep the internal queue healthy.
    // Timeout of 0 means non-blocking!
    producer_->poll(0);

    spdlog::debug("KafkaProducer: enqueued tick for {} ({} bytes)", ticker, message.size());
    return true;
}

void KafkaProducer::poll(int timeout_ms) {
    if (producer_) {
        producer_->poll(timeout_ms);
    }
}

//  Delivery report callback implementation — logs the outcome of message
// deliveries, including any errors. This is called by librdkafka when a message
// is confirmed delivered or permanently failed.
void KafkaProducer::DeliveryReportCb::dr_cb(RdKafka::Message& message) {
    if (message.err() != RdKafka::ERR_NO_ERROR) {
        // Permanent delivery failure — the broker rejected the message or
        // the retry limit was exceeded.
        spdlog::error("KafkaProducer: delivery failed for key '{}': {}",
            message.key() ? *message.key() : "unknown",
            message.errstr());
    } else {
        spdlog::debug("KafkaProducer: delivered to partition {} offset {}",
            message.partition(), message.offset());
    }
}