#pragma once
#include <deque>
#include <stdexcept>
#include <string>

// MovingAverage calculates a Simple Moving Average (SMA) over a rolling
// window of price values. Each call to update() adds a new price, drops
// the oldest if the window is full, and returns the current average.

class MovingAverage {
public:
    // Constructs the moving average with the given window period.
    // Throws std::invalid_argument if period is zero.
    explicit MovingAverage(size_t period);

    // Adds a new price to the window and returns the current SMA.
    // Returns 0.0 until enough prices have been added to fill the window.
    double update(double price);

    // Returns true once the window has been filled with enough prices
    // to produce a statistically meaningful average.
    bool is_ready() const { return window_.size() >= period_; }

    // Returns the current SMA without adding a new price.
    // Returns 0.0 if the window is not yet full.
    double value() const;

    // Returns the configured window period.
    size_t period() const { return period_; }

    // Resets the window — clears all stored prices and running sum.
    void reset();

private:
    size_t period_;             // number of periods in the rolling window
    std::deque<double> window_; // rolling price window, front = oldest
    double running_sum_{ 0.0 }; // maintained incrementally to avoid O(n) recalc
};