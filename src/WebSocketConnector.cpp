#include "market/WebSocketConnector.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <openssl/ssl.h>

#include <chrono>
#include <stdexcept>
#include <utility>

namespace market {
namespace net = boost::asio;
namespace ssl = net::ssl;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;

class WebSocketConnector::Impl {
public:
    Impl(WebSocketConfig config, WebSocketMessageParser parser)
        : config_(std::move(config)),
          parser_(std::move(parser)),
          ssl_context_(ssl::context::tls_client),
          resolver_(io_context_),
          stream_(io_context_, ssl_context_) {
        if (config_.host.empty()) throw std::invalid_argument("WebSocket host must not be empty");
        if (!parser_) throw std::invalid_argument("WebSocket parser callback must not be empty");

        if (config_.verify_peer) {
            ssl_context_.set_default_verify_paths();
            stream_.next_layer().set_verify_mode(ssl::verify_peer);
            stream_.next_layer().set_verify_callback(ssl::host_name_verification(config_.host));
        } else {
            stream_.next_layer().set_verify_mode(ssl::verify_none);
        }

        if (!SSL_set_tlsext_host_name(stream_.next_layer().native_handle(), config_.host.c_str())) {
            throw std::runtime_error("failed to set TLS SNI hostname");
        }

        const auto endpoints = resolver_.resolve(config_.host, config_.port);
        beast::get_lowest_layer(stream_).expires_after(std::chrono::seconds(30));
        beast::get_lowest_layer(stream_).connect(endpoints);
        stream_.next_layer().handshake(ssl::stream_base::client);
        beast::get_lowest_layer(stream_).expires_never();
        stream_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
        stream_.set_option(websocket::stream_base::decorator([](websocket::request_type& request) {
            request.set(boost::beast::http::field::user_agent, "market-order-book-engine/0.1");
        }));
        stream_.handshake(config_.host + ':' + config_.port, config_.target);
        if (!config_.subscription_message.empty()) {
            stream_.write(net::buffer(config_.subscription_message));
        }
    }

    ~Impl() {
        beast::error_code error;
        stream_.close(websocket::close_code::normal, error);
    }

    bool next(MarketDataEvent& event) {
        for (;;) {
            beast::error_code error;
            stream_.read(buffer_, error);
            if (error == websocket::error::closed) return false;
            if (error) throw beast::system_error(error);
            const auto message = beast::buffers_to_string(buffer_.data());
            buffer_.consume(buffer_.size());
            if (parser_(message, event)) return true;
        }
    }

private:
    WebSocketConfig config_;
    WebSocketMessageParser parser_;
    net::io_context io_context_;
    ssl::context ssl_context_;
    tcp::resolver resolver_;
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> stream_;
    beast::flat_buffer buffer_;
};

WebSocketConnector::WebSocketConnector(WebSocketConfig config, WebSocketMessageParser parser)
    : impl_(std::make_unique<Impl>(std::move(config), std::move(parser))) {}

WebSocketConnector::~WebSocketConnector() = default;
WebSocketConnector::WebSocketConnector(WebSocketConnector&&) noexcept = default;
WebSocketConnector& WebSocketConnector::operator=(WebSocketConnector&&) noexcept = default;

bool WebSocketConnector::next(MarketDataEvent& event) { return impl_->next(event); }

}  // namespace market

