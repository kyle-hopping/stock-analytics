#include "rsi_calculator.hpp"
#include <numeric>
#include <algorithm>
#include <stdexcept>

RSICalculator::RSICalculator(size_t period) : period_(period) {
    if (period == 0) {
        throw std::invalid_argument("RSICalculator: period must be greater than zero");
    }
}

double RSICalculator::update(double price) {
    ++prices_seen_;

    // First price — no change yet, just store it and wait
    if (prices_seen_ == 1) {
        prev_price_ = price;
        return 0.0;
    }

    const double change = price - prev_price_;
    prev_price_ = price;

    if (!is_ready()) {
        // Still collecting the initial window of price changes needed to
        // seed Wilder's smoothed averages. Store each change until we have
        // exactly period_ changes (which requires period_ + 1 prices).
        initial_changes_.push_back(change);

        if (prices_seen_ == period_ + 1) {
            // We now have exactly period_ changes — compute the seed averages
            // as a simple average, then switch to Wilder's SMMA from here on
            initialise_averages(initial_changes_);
            initial_changes_.clear();
        }

        return 0.0;
    }

    // Wilder's Smoothed Moving Average
    // Wilder's SMMA applies a smoothing factor of 1/period to each new value.
    // This gives more weight to recent changes while retaining history — the
    // standard formula used in professional RSI implementations.
    const double gain = std::max(change, 0.0); // positive change or zero
    const double loss = std::max(-change, 0.0); // negative change (made positive) or zero

    avg_gain_ = ((avg_gain_ * (period_ - 1)) + gain) / period_;
    avg_loss_ = ((avg_loss_ * (period_ - 1)) + loss) / period_;

    // Compute RSI from smoothed averages
    if (avg_loss_ == 0.0) {
        // No losses at all — RSI is maximum (pure upward momentum)
        rsi_ = 100.0;
    } else {
        // RSI = 100 - (100 / (1 + RS)) where RS = avg_gain / avg_loss
        const double rs = avg_gain_ / avg_loss_;
        rsi_ = 100.0 - (100.0 / (1.0 + rs));
    }

    return rsi_;
}

void RSICalculator::initialise_averages(const std::deque<double>& changes) {
    // Seed Wilder's averages with a simple mean of the first period_ changes.
    // This is the standard initialisation method — subsequent updates use SMMA.
    double total_gain = 0.0;
    double total_loss = 0.0;

    for (const double change : changes) {
        if (change > 0.0) {
            total_gain += change;
        } else {
            total_loss += -change; // convert loss to positive value
        }
    }

    avg_gain_ = total_gain / static_cast<double>(period_);
    avg_loss_ = total_loss / static_cast<double>(period_);

    // Compute the initial RSI from the seeded averages
    if (avg_loss_ == 0.0) {
        rsi_ = 100.0;
    } else {
        const double rs = avg_gain_ / avg_loss_;
        rsi_ = 100.0 - (100.0 / (1.0 + rs));
    }
}

void RSICalculator::reset() {
    prices_seen_ = 0;
    prev_price_  = 0.0;
    avg_gain_    = 0.0;
    avg_loss_    = 0.0;
    rsi_         = 0.0;
    initial_changes_.clear();
}