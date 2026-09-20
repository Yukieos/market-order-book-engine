#pragma once

#include "market/DataConnector.hpp"

#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

namespace market {

MarketDataEvent parse_csv_event(std::string_view line);

class CSVReplayConnector final : public DataConnector {
public:
    explicit CSVReplayConnector(const std::filesystem::path& path);

    [[nodiscard]] bool is_open() const noexcept;
    bool next(MarketDataEvent& event) override;
    [[nodiscard]] std::size_t line_number() const noexcept { return line_number_; }

private:
    std::ifstream input_;
    std::string line_;
    std::size_t line_number_{0};
};

}  // namespace market
