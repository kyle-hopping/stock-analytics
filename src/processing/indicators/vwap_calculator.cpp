#include "vwap_calculator.hpp"

double VWAPCalculator::update(double price, double high, double low, int64_t volume) {
    // Guard against zero or negative volume — can occur with bad API data
    if (volume <= 0) {
        return value();
    }

    // Guard against invalid price data
    if (price <= 0.0 || high <= 0.0 || low <= 0.0) {
        return value();
    }

    // Typical price is the standard three-point average used in VWAP.
    // It captures the full range of the period rather than just the close.
    const double typical_price = (high + low + price) / 3.0;

    // Accumulate weighted price and volume totals for the session.
    // These running totals allow O(1) updates without storing tick history.
    cumulative_tp_volume_ += typical_price * static_cast<double>(volume);
    cumulative_volume_    += static_cast<double>(volume);

    return value();
}

double VWAPCalculator::value() const {
    // Avoid division by zero on first tick or after reset
    if (cumulative_volume_ == 0.0) {
        return 0.0;
    }

    return cumulative_tp_volume_ / cumulative_volume_;
}

void VWAPCalculator::reset() {
    // Reset session totals — called at market open each day so VWAP
    // reflects only the current session's trading activity
    cumulative_tp_volume_ = 0.0;
    cumulative_volume_    = 0.0;
}