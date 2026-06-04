#pragma once
#include <deque>
#include <stdexcept>

// RSICalculator computes the Relative Strength Index (RSI) over a rolling
// window of price changes. RSI measures the speed and magnitude of recent
// price movements on a scale of 0 to 100.

// Interpretation:
//   RSI > 70  — asset may be overbought (potential sell signal)
//   RSI < 30  — asset may be oversold  (potential buy signal)
//   RSI = 50  — neutral momentum

// Uses Wilder's Smoothed Moving Average (SMMA) — the industry standard
// method used by most trading platforms and charting tools.
class RSICalculator {
public:
    // Constructs the RSI calculator with the given period.
    // Standard period is 14 as defined by J. Welles Wilder.
    // Throws std::invalid_argument if period is zero.
    explicit RSICalculator(size_t period = 14);

    // Feeds a new price into the calculator and returns the current RSI.
    // Returns 0.0 until enough prices have been seen to fill the window.
    double update(double price);

    // Returns true once enough prices have been seen to produce a valid RSI.
    // Requires period + 1 prices — one extra to compute the first price change.
    bool is_ready() const { return prices_seen_ > period_; }

    // Returns the current RSI value without adding a new price.
    double value() const { return rsi_; }

    // Returns the configured period.
    size_t period() const { return period_; }

    // Resets all internal state — clears price history and averages.
    void reset();

private:
    // Calculates Wilder's SMMA-based RSI from the initial window of price changes.
    // Called once after the first full period of data has been collected.
    void initialise_averages(const std::deque<double>& changes);

    size_t period_;              // RSI calculation period (default 14)
    size_t prices_seen_{ 0 };    // total prices fed in since construction/reset
    double prev_price_{ 0.0 };   // previous price for computing change
    double avg_gain_{ 0.0 };     // Wilder's smoothed average gain
    double avg_loss_{ 0.0 };     // Wilder's smoothed average loss
    double rsi_{ 0.0 };          // current RSI value

    // Stores initial price changes until we have enough for the first average.
    // Cleared after initialise_averages() is called.
    std::deque<double> initial_changes_;
};