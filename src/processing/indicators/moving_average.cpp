#include "moving_average.hpp"

MovingAverage::MovingAverage(size_t period) : period_(period) {
    if (period == 0) {
        throw std::invalid_argument("MovingAverage: period must be greater than zero");
    }
}

double MovingAverage::update(double price) {
    // Add the new price to the window and update the running sum incrementally.
    // Maintaining a running sum avoids iterating the entire window every update
    // which keeps this O(1) regardless of window size.
    running_sum_ += price;
    window_.push_back(price);

    // If the window exceeds the configured period, drop the oldest price
    if (window_.size() > period_) {
        running_sum_ -= window_.front();
        window_.pop_front();
    }

    return value();
}

double MovingAverage::value() const {
    // Return 0.0 until we have enough prices to fill the window — a partial
    // average would be misleading for trading decisions
    if (window_.size() < period_) {
        return 0.0;
    }

    return running_sum_ / static_cast<double>(period_);
}

void MovingAverage::reset() {
    window_.clear();
    running_sum_ = 0.0;
}