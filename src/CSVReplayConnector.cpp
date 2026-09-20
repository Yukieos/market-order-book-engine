#include "market/CSVReplayConnector.hpp"

#include <array>
#include <charconv>
#include <stdexcept>
#include <string_view>

namespace market {
namespace {

std::array<std::string_view, 7> split_row(std::string_view line) {
    std::array<std::string_view, 7> fields{};
    std::size_t begin = 0;
    for (std::size_t i = 0; i < fields.size(); ++i) {
        const auto end = line.find(',', begin);
        if (i + 1 == fields.size()) {
            if (end != std::string::npos) throw std::runtime_error("too many CSV fields");
            fields[i] = line.substr(begin);
        } else {
            if (end == std::string::npos) throw std::runtime_error("too few CSV fields");
            fields[i] = line.substr(begin, end - begin);
            begin = end + 1;
        }
    }
    return fields;
}

template <typename T>
T parse_integer(std::string_view value) {
    T result{};
    const auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (error != std::errc{} || ptr != value.data() + value.size()) {
        throw std::runtime_error("invalid integer field");
    }
    return result;
}

EventType parse_type(std::string_view value) {
    if (value == "add") return EventType::Add;
    if (value == "modify") return EventType::Modify;
    if (value == "cancel") return EventType::Cancel;
    if (value == "trade") return EventType::Trade;
    throw std::runtime_error("invalid event type");
}

Side parse_side(std::string_view value) {
    if (value == "buy") return Side::Buy;
    if (value == "sell") return Side::Sell;
    throw std::runtime_error("invalid side");
}

}  // namespace

MarketDataEvent parse_csv_event(std::string_view line) {
    const auto fields = split_row(line);
    return MarketDataEvent{
        .sequence = parse_integer<std::uint64_t>(fields[0]),
        .exchange_timestamp_ns = parse_integer<std::uint64_t>(fields[1]),
        .type = parse_type(fields[2]),
        .order_id = parse_integer<OrderId>(fields[3]),
        .side = parse_side(fields[4]),
        .price_ticks = parse_integer<Price>(fields[5]),
        .quantity = parse_integer<Quantity>(fields[6]),
    };
}

CSVReplayConnector::CSVReplayConnector(const std::filesystem::path& path) : input_(path) {
    if (!input_) throw std::runtime_error("cannot open CSV file: " + path.string());
}

bool CSVReplayConnector::is_open() const noexcept { return input_.is_open(); }

bool CSVReplayConnector::next(MarketDataEvent& event) {
    while (std::getline(input_, line_)) {
        ++line_number_;
        if (line_.empty() || line_[0] == '#') continue;
        if (line_.starts_with("sequence,")) continue;
        try {
            event = parse_csv_event(line_);
            return true;
        } catch (const std::exception& error) {
            throw std::runtime_error("CSV line " + std::to_string(line_number_) + ": " + error.what());
        }
    }
    return false;
}

}  // namespace market
