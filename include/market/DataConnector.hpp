#pragma once

#include "market/MarketDataEvent.hpp"

namespace market {

class DataConnector {
public:
    virtual ~DataConnector() = default;
    virtual bool next(MarketDataEvent& event) = 0;
};

}  // namespace market

