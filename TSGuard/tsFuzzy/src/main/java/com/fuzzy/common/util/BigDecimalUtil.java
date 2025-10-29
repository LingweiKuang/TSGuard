package com.fuzzy.common.util;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public class BigDecimalUtil {
    private static final int scale = 6;

    public static boolean isDouble(BigDecimal number) {
        number = number.stripTrailingZeros();
        return number.scale() >= Math.pow(10, -scale);
    }

    public static BigDecimal sqrt(BigDecimal value, int scale) {
        if (value.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Negative value: " + value);
        }
        if (value.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        // 初始猜测
        BigDecimal guess = value.divide(BigDecimal.valueOf(2), scale, RoundingMode.HALF_UP);
        BigDecimal lastGuess;
        BigDecimal epsilon = BigDecimal.valueOf(1).scaleByPowerOfTen(-scale);

        // 牛顿迭代公式
        do {
            lastGuess = guess;
            guess = value.divide(guess, scale, RoundingMode.HALF_UP)
                    .add(guess)
                    .divide(BigDecimal.valueOf(2), scale, RoundingMode.HALF_UP);
        } while (lastGuess.subtract(guess).abs().compareTo(epsilon) > 0);

        return guess;
    }


    /**
     * BigDecimal 执行 atan2 三角二元计算, y atan2 x
     *
     * @param y
     * @param x
     * @return
     */
    public static BigDecimal atan2(BigDecimal y, BigDecimal x, int precision) {
        double dy = y.doubleValue();
        double dx = x.doubleValue();
        double result = Math.atan2(dy, dx);
        MathContext mc = new MathContext(precision);
        return new BigDecimal(result, mc);
    }
}
