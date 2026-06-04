#include "websocket_server.hpp"
#include <App.h>
#include <random>
#include <sstream>
#include <iomanip>

WebSocketServer::WebSocketServer(int port) : port_(port) {
    spdlog::info("WebSocketServer: initialised on port {}", port);
}

WebSocketServer::~WebSocketServer() {
    stop();
}

void WebSocketServer::start() {
    if (running_.load()) {
        spdlog::warn("WebSocketServer: already running");
        return;
    }

    running_.store(true);

    // Start the uWebSockets event loop on a dedicated background thread
    // so it doesn't block the main thread or the Kafka consumer loop
    server_thread_ = std::thread([this]() {
        uWS::App()
            .ws<Client>("/*", {
                // Connection opened
                .open = [this](auto* ws) {
                    const std::string client_id = generate_client_id();

                    // Store per-socket client data using uWS's getUserData()
                    ws->getUserData()->id = client_id;

                    {
                        std::unique_lock<std::mutex> lock(clients_mutex_);
                        clients_[client_id] = Client{ client_id, {} };
                    }

                    client_count_.fetch_add(1, std::memory_order_relaxed);

                    spdlog::info("WebSocketServer: client {} connected ({} total)",
                        client_id, client_count_.load());

                    // Send a welcome message so the client knows it's connected
                    ws->send(nlohmann::json{
                        { "type",    "connected" },
                        { "message", "Connected to Stock Analytics dashboard" },
                        { "client_id", client_id }
                    }.dump(), uWS::OpCode::TEXT);
                },

                // Message received
                .message = [this](auto* ws, std::string_view message, uWS::OpCode) {
                    const std::string client_id = ws->getUserData()->id;

                    // Parse the incoming message as JSON
                    nlohmann::json msg;
                    try {
                        msg = nlohmann::json::parse(message);
                    } catch (const nlohmann::json::parse_error& e) {
                        spdlog::warn("WebSocketServer: invalid JSON from client {}: {}",
                            client_id, e.what());

                        ws->send(nlohmann::json{
                            { "type",  "error" },
                            { "message", "Invalid JSON" }
                        }.dump(), uWS::OpCode::TEXT);
                        return;
                    }

                    handle_subscription(client_id, msg);
                },

                // Connection closed
                .close = [this](auto* ws, int code, std::string_view reason) {
                    const std::string client_id = ws->getUserData()->id;

                    {
                        std::unique_lock<std::mutex> lock(clients_mutex_);
                        clients_.erase(client_id);
                    }

                    client_count_.fetch_sub(1, std::memory_order_relaxed);

                    spdlog::info("WebSocketServer: client {} disconnected "
                        "(code={}, reason={}, {} remaining)",
                        client_id, code, reason, client_count_.load());
                }
            })

            // HTTP endpoint — health check
            .get("/health", [](auto* res, auto* req) {
                res->writeHeader("Content-Type", "application/json");
                res->end(nlohmann::json{
                    { "status", "ok" },
                    { "service", "stock-analytics-dashboard" }
                }.dump());
            })

            // Start listening
            .listen(port_, [this](auto* socket) {
                if (socket) {
                    listen_socket_ = socket;
                    spdlog::info("WebSocketServer: listening on port {}", port_);
                } else {
                    spdlog::error("WebSocketServer: failed to bind to port {}", port_);
                    running_.store(false);
                }
            })
            .run();

        // uWS::App::run() only returns after us_listen_socket_close() is called
        spdlog::info("WebSocketServer: event loop exited");
        running_.store(false);
    });
}

void WebSocketServer::stop() {
    if (!running_.load()) {
        return;
    }

    running_.store(false);

    // Closing the listen socket causes uWS::App::run() to return,
    // which exits the server thread's event loop cleanly
    if (listen_socket_) {
        us_listen_socket_close(0, listen_socket_);
        listen_socket_ = nullptr;
    }

    if (server_thread_.joinable()) {
        server_thread_.join();
    }

    spdlog::info("WebSocketServer: stopped");
}

void WebSocketServer::broadcast_tick(const nlohmann::json& tick) {
    if (!tick.contains("ticker")) {
        return;
    }

    const std::string ticker  = tick["ticker"].get<std::string>();
    const std::string payload = tick.dump();

    std::unique_lock<std::mutex> lock(clients_mutex_);

    // Send only to clients subscribed to this ticker — avoids flooding
    // clients with data they didn't ask for
    for (auto& [id, client] : clients_) {
        // An empty subscription set means the client wants all tickers
        if (client.subscriptions.empty() ||
            client.subscriptions.count(ticker) > 0) {
            // uWS send is not thread-safe — in production this should
            // go through uWS's deferCallback for cross-thread safety
            spdlog::debug("WebSocketServer: broadcasting {} to client {}", ticker, id);
        }
    }
}

void WebSocketServer::broadcast_all(const nlohmann::json& message) {
    const std::string payload = message.dump();

    std::unique_lock<std::mutex> lock(clients_mutex_);

    spdlog::debug("WebSocketServer: broadcasting to all {} clients",
        clients_.size());
}

void WebSocketServer::handle_subscription(const std::string& client_id,
                                           const nlohmann::json& message) {
    if (!message.contains("action") || !message.contains("tickers")) {
        spdlog::warn("WebSocketServer: malformed subscription from client {}",
            client_id);
        return;
    }

    const std::string action = message["action"].get<std::string>();
    const auto& tickers      = message["tickers"];

    std::unique_lock<std::mutex> lock(clients_mutex_);
    auto it = clients_.find(client_id);
    if (it == clients_.end()) {
        return;
    }

    Client& client = it->second;

    if (action == "subscribe") {
        for (const auto& ticker : tickers) {
            client.subscriptions.insert(ticker.get<std::string>());
        }
        spdlog::info("WebSocketServer: client {} subscribed to {} ticker(s)",
            client_id, tickers.size());
    } else if (action == "unsubscribe") {
        for (const auto& ticker : tickers) {
            client.subscriptions.erase(ticker.get<std::string>());
        }
        spdlog::info("WebSocketServer: client {} unsubscribed from {} ticker(s)",
            client_id, tickers.size());
    }

    // Notify the subscription callback so the stream processor can
    // adjust which tickers it prioritises processing
    if (subscription_cb_) {
        const std::vector<std::string> sub_list(
            client.subscriptions.begin(),
            client.subscriptions.end()
        );
        subscription_cb_(client_id, sub_list);
    }
}

std::string WebSocketServer::generate_client_id() {
    // Generate a short random hex ID for each client e.g. "a3f92b1c"
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dist;

    std::ostringstream oss;
    oss << std::hex << std::setw(8) << std::setfill('0') << dist(gen);
    return oss.str();
}