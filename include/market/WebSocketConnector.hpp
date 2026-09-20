#pragma once

#include "market/DataConnector.hpp"

#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace market {

struct WebSocketConfig {
    std::string host;
    std::string port{"443"};
    std::string target{"/"};
    std::string subscription_message;
    bool verify_peer{true};
};

// The parser returns true when a frame produced one normalized event. Returning
// false skips control, heartbeat, subscription-ack, or otherwise irrelevant frames.
using WebSocketMessageParser = std::function<bool(std::string_view, MarketDataEvent&)>;

class WebSocketConnector final : public DataConnector {
public:
    WebSocketConnector(WebSocketConfig config, WebSocketMessageParser parser);
    ~WebSocketConnector() override;

    WebSocketConnector(const WebSocketConnector&) = delete;
    WebSocketConnector& operator=(const WebSocketConnector&) = delete;
    WebSocketConnector(WebSocketConnector&&) noexcept;
    WebSocketConnector& operator=(WebSocketConnector&&) noexcept;

    bool next(MarketDataEvent& event) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace market

