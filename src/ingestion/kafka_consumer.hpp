#pragma once
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

// Interface for subscribing to Kafka topics and processing incoming stock
// tick messages. Runs a poll loop on a thread and dispatches each message to a
// user-supplied callback for processing.
class KafkaConsumer {
public:
    // Callback type invoked for every successfully decoded message.
    // Receives the ticker symbol (message key) and the parsed JSON payload.
    using MessageCallback = std::function<void(const std::string& ticker,
                                               const nlohmann::json& payload)>;

    // Constructor - connects the consumer to the given broker.
    // group_id identifies this consumer within a Kafka consumer group —
    // all consumers sharing the same group_id share the partition workload.
    // Throws std::runtime_error if the connection cannot be established.
    explicit KafkaConsumer(const std::string& broker,
                           const std::string& group_id);

    // Destructor stops the poll loop and unsubscribes from all topics.
    ~KafkaConsumer();

    // Subscribes to one or more Kafka topics.
    // Throws std::runtime_error if subscription fails.
    void subscribe(const std::vector<std::string>& topics);

    // Registers the callback that will be invoked for every incoming message.
    // Has to be set before calling start().
    void set_callback(MessageCallback callback) { callback_ = std::move(callback); }

    // Starts the poll loop — blocks the calling thread and continuously polls
    // Kafka for new messages, dispatching each to the registered callback.
    // Call stop() from another thread to exit the loop.
    void start();

    // Signals the poll loop to stop after finishing the current message.
    // Safe to call from any thread?
    void stop() { running_.store(false); }

    // Returns true if the poll loop is currently running.
    bool is_running() const { return running_.load(); }

private:
    // Processes a single message received from Kafka. Deserialises the JSON
    // payload and invokes the registered callback.
    void handle_message(RdKafka::Message& message);

    // Rebalance callback — called by librdkafka when partitions are assigned
    // or revoked due to consumers joining or leaving the group.
    class RebalanceCb : public RdKafka::RebalanceCb {
    public:
        void rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
                          std::vector<RdKafka::TopicPartition*>& partitions) override;
    };

    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;  // librdkafka consumer handle
    MessageCallback callback_;                          // user-supplied message handler
    RebalanceCb rebalance_cb_;                          // partition rebalance handler
    std::atomic<bool> running_{ false };                // controls the poll loop
    std::string broker_;                                // broker address for logging
    std::string group_id_;                              // consumer group id for logging

    // Poll timeout — how long to block waiting for a message before returning
    // control to check running_. Supposedly shorter = more responsive
    // to stop(), longer = less CPU usage when the topic is quiet. Can tune later.
    static constexpr int POLL_TIMEOUT_MS = 100;
};