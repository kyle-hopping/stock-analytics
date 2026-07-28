#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include "processing/indicators/moving_average.hpp"
#include "processing/indicators/rsi_calculator.hpp"
#include "processing/indicators/vwap_calculator.hpp"

// StreamProcessor indicator unit tests
// The StreamProcessor itself requires live Kafka so we test its core
// indicator calculation logic directly here. Each indicator is tested
// independently and then together as they would be used in process_tick().

// MovingAverage tests

// is_ready() only flips to true once exactly `period` values have been fed in
TEST(MovingAverageTest, NotReadyUntilWindowFilled) {
    MovingAverage sma(3);

    EXPECT_FALSE(sma.is_ready());
    sma.update(10.0);
    EXPECT_FALSE(sma.is_ready());
    sma.update(20.0);
    EXPECT_FALSE(sma.is_ready());
    sma.update(30.0);
    EXPECT_TRUE(sma.is_ready());
}

// update() returns 0.0 for each call before the window is fully filled
TEST(MovingAverageTest, ReturnsZeroBeforeWindowFilled) {
    MovingAverage sma(3);

    EXPECT_DOUBLE_EQ(sma.update(10.0), 0.0);
    EXPECT_DOUBLE_EQ(sma.update(20.0), 0.0);
}

// Once the window fills, update() returns the correct arithmetic mean of the values
TEST(MovingAverageTest, CalculatesCorrectAverageWhenReady) {
    MovingAverage sma(3);

    sma.update(10.0);
    sma.update(20.0);
    double result = sma.update(30.0); // window full: (10+20+30)/3 = 20.0

    EXPECT_NEAR(result, 20.0, 0.001);
}

// Pushing a new value past a full window evicts the oldest value from the average
TEST(MovingAverageTest, RollingWindowDropsOldestValue) {
    MovingAverage sma(3);

    sma.update(10.0);
    sma.update(20.0);
    sma.update(30.0); // window: [10, 20, 30], avg = 20.0
    double result = sma.update(40.0); // window: [20, 30, 40], avg = 30.0

    EXPECT_NEAR(result, 30.0, 0.001);
}

// Feeding the same price repeatedly keeps the average equal to that price once ready
TEST(MovingAverageTest, ConstantPricesProduceConstantAverage) {
    MovingAverage sma(5);

    for (int i = 0; i < 10; ++i) {
        double result = sma.update(100.0);
        if (sma.is_ready()) {
            EXPECT_NEAR(result, 100.0, 0.001);
        }
    }
}

// reset() clears the window and value, returning the average to a not-ready, zero state
TEST(MovingAverageTest, ResetClearsWindowAndSum) {
    MovingAverage sma(3);

    sma.update(10.0);
    sma.update(20.0);
    sma.update(30.0);
    EXPECT_TRUE(sma.is_ready());

    sma.reset();
    EXPECT_FALSE(sma.is_ready());
    EXPECT_DOUBLE_EQ(sma.value(), 0.0);
}

// Constructing a MovingAverage with period 0 is rejected with std::invalid_argument
TEST(MovingAverageTest, ThrowsOnZeroPeriod) {
    EXPECT_THROW(MovingAverage sma(0), std::invalid_argument);
}

// A period-1 moving average is ready after a single update and equals that value
TEST(MovingAverageTest, PeriodOneAlwaysReady) {
    MovingAverage sma(1);
    double result = sma.update(42.0);

    EXPECT_TRUE(sma.is_ready());
    EXPECT_NEAR(result, 42.0, 0.001);
}

// RSICalculator tests

// RSI needs period + 1 price updates (15 for period 14) before is_ready() returns true
TEST(RSICalculatorTest, NotReadyUntilEnoughPrices) {
    RSICalculator rsi(14);

    // Needs period + 1 = 15 prices before is_ready() returns true
    for (int i = 0; i < 14; ++i) {
        rsi.update(100.0 + i);
        EXPECT_FALSE(rsi.is_ready());
    }

    rsi.update(114.0);
    EXPECT_TRUE(rsi.is_ready());
}

// update() returns 0.0 for each call before enough prices have been fed
TEST(RSICalculatorTest, ReturnsZeroBeforeReady) {
    RSICalculator rsi(14);

    for (int i = 0; i < 14; ++i) {
        EXPECT_DOUBLE_EQ(rsi.update(100.0 + i), 0.0);
    }
}

// Strictly increasing prices (no losses) drive RSI to 100
TEST(RSICalculatorTest, RSIIs100WithOnlyGains) {
    RSICalculator rsi(14);

    // Feed strictly increasing prices — no losses at all
    for (int i = 0; i < 20; ++i) {
        rsi.update(100.0 + i);
    }

    // With only gains avg_loss = 0 so RSI = 100
    EXPECT_NEAR(rsi.value(), 100.0, 0.001);
}

// Strictly decreasing prices (no gains) drive RSI toward 0
TEST(RSICalculatorTest, RSIIs0WithOnlyLosses) {
    RSICalculator rsi(14);

    // Feed strictly decreasing prices — no gains at all
    for (int i = 0; i < 20; ++i) {
        rsi.update(200.0 - i);
    }

    // With only losses avg_gain = 0 so RSI approaches 0
    EXPECT_LT(rsi.value(), 10.0);
}

// Equal alternating gains and losses keep RSI near the neutral midpoint of 50
TEST(RSICalculatorTest, RSIIsNearFiftyWithNeutralMovement) {
    RSICalculator rsi(14);

    // Alternating up and down by equal amounts — RSI should be near 50
    for (int i = 0; i < 30; ++i) {
        rsi.update(i % 2 == 0 ? 100.0 : 101.0);
    }

    EXPECT_GT(rsi.value(), 40.0);
    EXPECT_LT(rsi.value(), 60.0);
}

// RSI stays within the valid [0, 100] range across a mixed sequence of price moves
TEST(RSICalculatorTest, RSIAlwaysBetweenZeroAndHundred) {
    RSICalculator rsi(14);

    // Feed random-ish prices and verify RSI stays in valid range
    std::vector<double> prices = {
        100, 102, 101, 103, 105, 104, 106, 103,
        101, 99,  98,  100, 102, 104, 103, 105,
        107, 106, 108, 110, 109, 111, 110, 108
    };

    for (double price : prices) {
        double val = rsi.update(price);
        if (rsi.is_ready()) {
            EXPECT_GE(val, 0.0);
            EXPECT_LE(val, 100.0);
        }
    }
}

// reset() clears RSI's internal state, returning it to not-ready with value 0
TEST(RSICalculatorTest, ResetClearsAllState) {
    RSICalculator rsi(14);

    for (int i = 0; i < 20; ++i) {
        rsi.update(100.0 + i);
    }
    EXPECT_TRUE(rsi.is_ready());

    rsi.reset();
    EXPECT_FALSE(rsi.is_ready());
    EXPECT_DOUBLE_EQ(rsi.value(), 0.0);
}

// Constructing an RSICalculator with period 0 is rejected with std::invalid_argument
TEST(RSICalculatorTest, ThrowsOnZeroPeriod) {
    EXPECT_THROW(RSICalculator rsi(0), std::invalid_argument);
}

// VWAPCalculator tests

// A freshly constructed VWAPCalculator is not ready and reports a zero value
TEST(VWAPCalculatorTest, NotReadyBeforeFirstTick) {
    VWAPCalculator vwap;
    EXPECT_FALSE(vwap.is_ready());
    EXPECT_DOUBLE_EQ(vwap.value(), 0.0);
}

// A single update() call is enough to make VWAP ready
TEST(VWAPCalculatorTest, ReadyAfterFirstTick) {
    VWAPCalculator vwap;
    vwap.update(100.0, 101.0, 99.0, 1000);
    EXPECT_TRUE(vwap.is_ready());
}

// VWAP on a single tick equals the typical price (high+low+close)/3
TEST(VWAPCalculatorTest, CalculatesCorrectTypicalPrice) {
    VWAPCalculator vwap;

    // Typical price = (high + low + close) / 3
    // = (101 + 99 + 100) / 3 = 100.0
    // VWAP = (100.0 * 1000) / 1000 = 100.0
    double result = vwap.update(100.0, 101.0, 99.0, 1000);

    EXPECT_NEAR(result, 100.0, 0.001);
}

// VWAP weights each tick's price by its volume, so higher-volume ticks pull the average more
TEST(VWAPCalculatorTest, WeightsHigherVolumeMoreHeavily) {
    VWAPCalculator vwap;

    // Tick 1: price=100, volume=100  → contribution=10000
    // Tick 2: price=200, volume=900  → contribution=180000
    // VWAP = 190000/1000 = 190.0
    vwap.update(100.0, 101.0, 99.0, 100);
    double result = vwap.update(200.0, 202.0, 198.0, 900);

    EXPECT_NEAR(result, 190.0, 1.0);
}

// Repeated ticks at the same price/volume keep VWAP steady at that price
TEST(VWAPCalculatorTest, ConstantPriceProducesConstantVWAP) {
    VWAPCalculator vwap;

    for (int i = 0; i < 10; ++i) {
        double result = vwap.update(150.0, 151.0, 149.0, 1000);
        EXPECT_NEAR(result, 150.0, 0.001);
    }
}

// A zero-volume tick contributes nothing, leaving VWAP unchanged
TEST(VWAPCalculatorTest, IgnoresZeroVolumeTicksGracefully) {
    VWAPCalculator vwap;

    double first  = vwap.update(100.0, 101.0, 99.0, 1000);
    double second = vwap.update(200.0, 201.0, 199.0, 0); // zero volume — skipped

    // VWAP should not change after zero-volume tick
    EXPECT_NEAR(second, first, 0.001);
}

// reset() clears VWAP's accumulated totals, returning it to not-ready with value 0
TEST(VWAPCalculatorTest, ResetClearsSessionTotals) {
    VWAPCalculator vwap;

    vwap.update(100.0, 101.0, 99.0, 1000);
    EXPECT_TRUE(vwap.is_ready());

    vwap.reset();
    EXPECT_FALSE(vwap.is_ready());
    EXPECT_DOUBLE_EQ(vwap.value(), 0.0);
}

// Combined indicator tests

// SMA-20, SMA-50, RSI-14, and VWAP all become ready and hold valid values when fed the same price stream, as process_tick() would drive them
TEST(StreamProcessorIndicatorTest, AllIndicatorsUpdateIndependently) {
    // Simulates what process_tick() does with a sequence of prices
    MovingAverage  sma_20(20);
    MovingAverage  sma_50(50);
    RSICalculator  rsi_14(14);
    VWAPCalculator vwap;

    // Feed 60 prices — enough for all indicators to be ready
    for (int i = 0; i < 60; ++i) {
        const double price = 100.0 + (i % 10); // oscillating prices
        sma_20.update(price);
        sma_50.update(price);
        rsi_14.update(price);
        vwap.update(price, price * 1.01, price * 0.99, 10000);
    }

    EXPECT_TRUE(sma_20.is_ready());
    EXPECT_TRUE(sma_50.is_ready());
    EXPECT_TRUE(rsi_14.is_ready());
    EXPECT_TRUE(vwap.is_ready());

    // All values should be in reasonable ranges
    EXPECT_GT(sma_20.value(), 0.0);
    EXPECT_GT(sma_50.value(), 0.0);
    EXPECT_GE(rsi_14.value(), 0.0);
    EXPECT_LE(rsi_14.value(), 100.0);
    EXPECT_GT(vwap.value(), 0.0);
}

// Separate MovingAverage instances per ticker keep their state fully independent
TEST(StreamProcessorIndicatorTest, DifferentTickersHaveIndependentState) {
    // Simulates two tickers with completely different indicator instances
    MovingAverage sma_aapl(5);
    MovingAverage sma_tsla(5);

    // AAPL prices around 180, TSLA around 250
    for (int i = 0; i < 5; ++i) {
        sma_aapl.update(180.0 + i);
        sma_tsla.update(250.0 + i);
    }

    // They should produce completely different averages
    EXPECT_NEAR(sma_aapl.value(), 182.0, 0.001);
    EXPECT_NEAR(sma_tsla.value(), 252.0, 0.001);
}