#pragma once
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

// Forward declare uWebSockets types to avoid pulling in the full header
// in every file that includes this one
struct us_listen_socket_t;

// WebSocketServer pushes real-time enriched tick data to connected browser
// clients over WebSocket. Every time a processed tick arrives from the
// stream processor it is immediately broadcast to all connected clients.

// Protocol:
//   Server → Client: JSON tick payload on every processed tick
//   Client → Server: JSON subscription message to filter by ticker

// Subscription message format (client sends):
//   { "action": "subscribe",   "tickers": ["AAPL", "TSLA"] }
//   { "action": "unsubscribe", "tickers": ["AAPL"] }

// Broadcast message format (server sends):
//   {
//     "ticker": "AAPL",
//     "price": 182.63,
//     "indicators": { "sma_20": 181.45, "rsi_14": 62.3, "vwap": 182.10 },
//     "timestamp": "2024-01-15T14:30:00Z"
//   }

class WebSocketServer {
public:
    // Callback invoked when a client subscribes to specific tickers.
    // Receives the client ID and the list of requested ticker symbols.
    using SubscriptionCallback = std::function<void(
        const std::string& client_id,
        const std::vector<std::string>& tickers)>;

    // Constructs the server on the given port.
    // port — TCP port to listen on, default 9001
    explicit WebSocketServer(int port = 9001);

    // Destructor stops the server and disconnects all clients.
    ~WebSocketServer();

    // Starts the WebSocket server on a dedicated thread.
    // Returns immediately — the server runs in the background.
    void start();

    // Stops the server and closes all active connections.
    void stop();

    // Broadcasts an enriched tick to all connected clients subscribed
    // to that ticker symbol. Thread-safe — safe to call from any thread.
    void broadcast_tick(const nlohmann::json& tick);

    // Broadcasts a message to all connected clients regardless of subscription.
    void broadcast_all(const nlohmann::json& message);

    // Registers a callback invoked when a client changes their subscriptions.
    void set_subscription_callback(SubscriptionCallback cb) {
        subscription_cb_ = std::move(cb);
    }

    // Returns the number of currently connected clients.
    size_t client_count() const { return client_count_.load(); }

    // Returns true if the server is currently running.
    bool is_running() const { return running_.load(); }

private:
    // Internal client state — tracks each connected WebSocket client
    struct Client {
        std::string id;                      // unique client identifier
        std::set<std::string> subscriptions; // ticker symbols this client wants
    };

    // Handles an incoming subscription/unsubscription message from a client.
    void handle_subscription(const std::string& client_id,
                              const nlohmann::json& message);

    // Generates a unique ID for a newly connected client.
    static std::string generate_client_id();

    int  port_;                               // TCP port to listen on
    std::atomic<bool>     running_{ false };  // server running state
    std::atomic<size_t>   client_count_{ 0 }; // connected client count
    std::thread           server_thread_;     // dedicated server thread
    SubscriptionCallback  subscription_cb_;   // client subscription handler

    // Client registry — maps client ID to client state.
    // Protected by clients_mutex_ for thread-safe access from
    // the broadcast thread and the uWS event loop thread.
    std::unordered_map<std::string, Client> clients_;
    mutable std::mutex clients_mutex_;

    // uWebSockets listen socket — held so we can close it on stop()
    us_listen_socket_t* listen_socket_{ nullptr };
};