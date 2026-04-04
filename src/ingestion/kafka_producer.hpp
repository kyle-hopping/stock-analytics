#pragma once
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <memory>
#include <string>

// Interface for publishing stock tick data to a Kafka topic. It handles
// connection setup, serialisation, delivery callbacks, and shutdown.
class KafkaProducer {
public:
    // Constructor - connects the producer to the given broker address.
    // Throws std::runtime_error if the connection cannot be established.
    explicit KafkaProducer(const std::string& broker, const std::string& topic);

    // Destructor - flushes messages before destroying the producer.
    ~KafkaProducer();

    // Publishes a JSON payload to the configured Kafka topic. The ticker
    // symbol is the message key so all ticks for the same stock land on the
    // same partition. Returns true if the message was enqueued successfully.
    bool publish(const std::string& ticker, const nlohmann::json& payload);

    // Polls the producer for events. Call this periodically to trigger delivery
    // callbacks and keep the internal queue from filling.
    void poll(int timeout_ms = 0);

    // Returns true if the producer is connected and ready to publish.
    bool is_connected() const { return producer_ != nullptr; }

private:
    // Delivery report callback — called by librdkafka when a message is
    // confirmed delivered or permanently failed. Logs the outcome.
    class DeliveryReportCb : public RdKafka::DeliveryReportCb {
    public:
        void dr_cb(RdKafka::Message& message) override;
    };

    std::unique_ptr<RdKafka::Producer> producer_;  // librdkafka producer handle
    std::unique_ptr<RdKafka::Topic> topic_;        // target topic handle
    DeliveryReportCb delivery_cb_;                 // delivery report callback
    std::string broker_;                           // broker address for logging
    std::string topic_name_;                       // topic name for logging
};