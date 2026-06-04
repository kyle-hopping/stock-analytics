#pragma once
#include <stdexcept>

// VWAPCalculator computes the Volume Weighted Average Price (VWAP) —
// the average price of a stock weighted by trading volume. VWAP is used
// by institutional traders as a benchmark to assess execution quality.

// Interpretation:
//   Price > VWAP — stock trading above average, bullish intraday signal
//   Price < VWAP — stock trading below average, bearish intraday signal
//   Price ≈ VWAP — fair value, no strong directional signal

// VWAP resets each trading session (daily). Call reset() at open
// to start a fresh calculation for the new session.

// Formula:
//   Typical Price  = (High + Low + Close) / 3
//   VWAP           = Σ(Typical Price × Volume) / Σ(Volume)

class VWAPCalculator {
public:
    VWAPCalculator() = default;

    // Feeds a new price bar into the calculator and returns the current VWAP.
    // price  — closing/last trade price for this tick
    // high   — highest price in this tick's period
    // low    — lowest price in this tick's period
    // volume — number of shares traded in this tick's period
    // Returns 0.0 if cumulative volume is zero.
    double update(double price, double high, double low, int64_t volume);

    // Returns the current VWAP value without adding a new tick.
    // Returns 0.0 if no ticks have been added yet.
    double value() const;

    // Returns true once at least one tick has been processed.
    bool is_ready() const { return cumulative_volume_ > 0; }

    // Resets the VWAP for a new trading session.
    // Should be called at market open each day.
    void reset();

private:
    // Running totals maintained across all ticks in the session.
    // Using double for cumulative_volume_ to avoid overflow on large volumes.
    double cumulative_tp_volume_{ 0.0 }; // Σ(typical price × volume)
    double cumulative_volume_{ 0.0 };    // Σ(volume)
};