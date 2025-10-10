package com.fuzzy.common.tsaf;

import com.fuzzy.Randomly;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.util.BigDecimalUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
@Slf4j
public class Equations {
    // 指定方程类型，可依据方程产出某时间戳下具体 value 值
    private final EquationType equationType;
    private List<BigDecimal> args;

    public Equations(EquationType equationType, List<BigDecimal> args) {
        this.equationType = equationType;
        this.args = args;
    }

    public static Equations randomGenEquations(TSAFDataType tsafDataType) {
        Randomly randomly = new Randomly();
        BigDecimal k, c;
        EquationType type;
        switch (tsafDataType) {
            case INT:
                // TODO
                k = Randomly.getBoolean() ? BigDecimal.valueOf(randomly.getLong(-50, 0)) :
                        BigDecimal.valueOf(randomly.getLong(1, 50));
                c = BigDecimal.valueOf(randomly.getLong(-100, 100));
                type = Randomly.fromOptions(EquationType.linearEquation);
                k = BigDecimal.ONE;
                c = BigDecimal.ZERO;
                break;
            case UINT:
                k = BigDecimal.valueOf(randomly.getLong(1, 50));
                c = BigDecimal.valueOf(randomly.getLong(0, 100));
                type = Randomly.fromOptions(EquationType.linearEquation);
                break;
            case BIGINT:
                k = Randomly.getBoolean() ? BigDecimal.valueOf(randomly.getLong(-10000, 0)) :
                        BigDecimal.valueOf(randomly.getLong(1, 10000));
                c = BigDecimal.valueOf(randomly.getLong(-100000, 100000));
                type = Randomly.fromOptions(EquationType.linearEquation);
                break;
            case UBIGINT:
                k = BigDecimal.valueOf(randomly.getLong(1, 10000));
                c = BigDecimal.valueOf(randomly.getLong(0, 100000));
                type = Randomly.fromOptions(EquationType.linearEquation);
                break;
            case DOUBLE:
                k = Randomly.getBoolean() ? BigDecimal.valueOf(randomly.getLong(-10000, 0)) :
                        BigDecimal.valueOf(randomly.getLong(1, 10000));
                c = BigDecimal.valueOf(randomly.getLong(-100000, 100000));
                type = Randomly.fromOptions(EquationType.linearEquation, EquationType.reciprocalFunctionEquation);
                if (type == EquationType.reciprocalFunctionEquation && k.compareTo(BigDecimal.ZERO) < 0) k = k.negate();
                break;
            default:
                throw new AssertionError();
        }

        // TODO 实验时暂时仅考虑线性函数
        if (TSAFDataType.DOUBLE.equals(tsafDataType)) {
            k = Randomly.getBoolean() ? BigDecimal.valueOf(randomly.getLong(-50, 0)) :
                    BigDecimal.valueOf(randomly.getLong(1, 50));
            c = BigDecimal.valueOf(randomly.getLong(-100, 100));
            type = Randomly.fromOptions(EquationType.linearEquation);
        }

        List<BigDecimal> args = new ArrayList<>(Arrays.asList(k, c));
        return new Equations(type, args);
    }

    public static Equations genBaseAndBaseEquations() {
        List<BigDecimal> args = new ArrayList<>(Arrays.asList(BigDecimal.ONE, BigDecimal.ZERO));
        return new Equations(EquationType.linearEquation, args);
    }

    public static Equations genReciprocalEquations(BigDecimal k, BigDecimal c) {
        List<BigDecimal> args = new ArrayList<>(Arrays.asList(k, c));
        return new Equations(EquationType.reciprocalFunctionEquation, args);
    }

    // 由时间戳获取value值
    public enum EquationType {
        linearEquation {
            // y = k * x + c
            @Override
            public BigDecimal apply(SamplingFrequency samplingFrequency, long timestamp, List<BigDecimal> args) {
                assert args.size() == 2;
                // 生成序号列进一步生成value值
                BigDecimal seq = timestampToBaseSeq(samplingFrequency, timestamp);
                BigDecimal k = args.get(0);
                BigDecimal c = args.get(1);
                return seq.multiply(k).add(c);
            }

            @Override
            public TimeSeriesConstraint transformTimeSeriesConstraint(TimeSeriesConstraint timeSeriesConstraint,
                                                                      List<BigDecimal> args) {
                assert args.size() == 2;
                BigDecimal k = args.get(0);
                BigDecimal c = args.get(1);

                // 不等点转换
                TimeSeriesConstraint transConstraintRes = new TimeSeriesConstraint(GlobalConstant.BASE_TIME_SERIES_NAME);
                List<BigDecimal> transNotEqualValues = new ArrayList<>();
                timeSeriesConstraint.getNotEqualValues().forEach(notEqualValue -> {
                    BigDecimal notEqualTransValue = notEqualValue.subtract(c).divide(k,
                            DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP);
                    transNotEqualValues.add(notEqualTransValue);
                });
                transConstraintRes.setNotEqualValues(transNotEqualValues);

                // 等值范围变换
                List<RangeConstraint> transRangeConstraint = new ArrayList<>();
                timeSeriesConstraint.getRangeConstraints().forEach(rangeConstraint -> {
                    BigDecimal v1 = rangeConstraint.getGreaterEqualValue().subtract(c)
                            .divide(k, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP);
                    BigDecimal v2 = rangeConstraint.getLessEqualValue().subtract(c)
                            .divide(k, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP);
                    BigDecimal rangeLeft = v1.min(v2);
                    BigDecimal rangeRight = v1.max(v2);
                    transRangeConstraint.add(new RangeConstraint(rangeLeft, rangeRight));
                });
                transConstraintRes.setRangeConstraints(transRangeConstraint);
                return transConstraintRes;
            }

            @Override
            public ConstraintValue.TimeSeriesValue transformBaseTimeSeriesValue(
                    BigDecimal factor, BigDecimal intercept, List<BigDecimal> args) {
                // 传参c1, 得到c0的因子和截距
                assert args.size() == 2;
                BigDecimal k = args.get(0);
                BigDecimal c = args.get(1);

                return new ConstraintValue.TimeSeriesValue(GlobalConstant.BASE_TIME_SERIES_NAME, false,
                        new ConstraintValue.ConstantValue(factor.multiply(k).toPlainString()),
                        new ConstraintValue.ConstantValue(intercept.add(factor.multiply(c)).toPlainString()));
            }
        },
        reciprocalFunctionEquation {
            // y = k / x + c
            @Override
            public BigDecimal apply(SamplingFrequency samplingFrequency, long timestamp, List<BigDecimal> args) {
                assert args.size() == 2;
                // 生成序号列进一步生成value值 -> 序号列大于0
                BigDecimal seq = timestampToBaseSeq(samplingFrequency, timestamp);
                BigDecimal k = args.get(0);
                BigDecimal c = args.get(1);
                return k.divide(seq, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP).add(c);
            }

            @Override
            public TimeSeriesConstraint transformTimeSeriesConstraint(TimeSeriesConstraint timeSeriesConstraint,
                                                                      List<BigDecimal> args) {
                assert args.size() == 2;
                BigDecimal k = args.get(0);
                BigDecimal c = args.get(1);
                // 暂时仅支持第一象限数值运算
                assert k.compareTo(BigDecimal.ZERO) > 0;

                // 不等点转换
                TimeSeriesConstraint transConstraintRes = new TimeSeriesConstraint(GlobalConstant.BASE_TIME_SERIES_NAME);
                List<BigDecimal> transNotEqualValues = new ArrayList<>();
                timeSeriesConstraint.getNotEqualValues().forEach(notEqualValue -> {
                    BigDecimal subVal = notEqualValue.subtract(c);
                    // 除以0, 无穷大, 必不相等
                    if (subVal.compareTo(BigDecimal.ZERO) == 0) return;
                    BigDecimal notEqualTransValue = k.divide(subVal,
                            DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP);
                    transNotEqualValues.add(notEqualTransValue);
                });

                // 等值范围变换
                List<RangeConstraint> transRangeConstraint = new ArrayList<>();
                timeSeriesConstraint.getRangeConstraints().forEach(rangeConstraint -> {
                    BigDecimal subtractV1 = rangeConstraint.getLessEqualValue().subtract(c);
                    BigDecimal subtractV2 = rangeConstraint.getGreaterEqualValue().subtract(c);
                    // 若存在除数为0的情况, res依据k的正负取值置为无穷大
                    BigDecimal maxV = BigDecimal.valueOf(k.compareTo(BigDecimal.ZERO) > 0 ? Long.MAX_VALUE : Long.MIN_VALUE);
                    BigDecimal res1 = maxV;
                    BigDecimal res2 = maxV;
                    ;
                    if (subtractV1.compareTo(BigDecimal.ZERO) != 0)
                        res1 = k.divide(subtractV1, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP);
                    if (subtractV2.compareTo(BigDecimal.ZERO) != 0)
                        res2 = k.divide(subtractV2, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP);
                    BigDecimal rangeLeft = res1.min(res2);
                    BigDecimal rangeRight = res1.max(res2);

                    // 左右范围区间均小于0 -> 退出(c0取值仅在第一象限)
                    if (rangeLeft.compareTo(BigDecimal.ZERO) < 0
                            && (rangeRight.compareTo(BigDecimal.ZERO) < 0 || (rangeRight.compareTo(BigDecimal.ZERO) == 0
                            && rangeConstraint.getGreaterEqualValue().compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0)))
                        return;
                        // 左边界小于0, 右边界大于0 -> rangeRight, 正无穷
                    else if (rangeLeft.compareTo(BigDecimal.ZERO) < 0 || (rangeLeft.compareTo(BigDecimal.ZERO) == 0
                            && rangeConstraint.getGreaterEqualValue().compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0))
                        transRangeConstraint.add(new RangeConstraint(rangeRight, BigDecimal.valueOf(Long.MAX_VALUE)));
                    else
                        transRangeConstraint.add(new RangeConstraint(rangeLeft, rangeRight));
                });
                transConstraintRes.setNotEqualValues(transNotEqualValues);
                transConstraintRes.setRangeConstraints(transRangeConstraint);
                return transConstraintRes;
            }

            @Override
            public ConstraintValue.TimeSeriesValue transformBaseTimeSeriesValue(
                    BigDecimal factor, BigDecimal intercept, List<BigDecimal> args) {
                // c1 = k / c0 + c 代入 factor * c1 + intercept
                assert args.size() == 2;
                BigDecimal k = args.get(0);
                BigDecimal c = args.get(1);
                return new ConstraintValue.TimeSeriesValue(GlobalConstant.BASE_TIME_SERIES_NAME, false,
                        new ConstraintValue.ConstantValue(factor.multiply(k).toPlainString()),
                        new ConstraintValue.ConstantValue(intercept.add(factor.multiply(c)).toPlainString()))
                        .transToReciprocalVal();
            }
        },
        quadraticEquation {
            // y = k * x^2 + c
            @Override
            public BigDecimal apply(SamplingFrequency samplingFrequency, long timestamp, List<BigDecimal> args) {
                assert args.size() == 2;
                // 生成序号列进一步生成value值
                BigDecimal seq = timestampToBaseSeq(samplingFrequency, timestamp);
                BigDecimal k = args.get(0);
                BigDecimal c = args.get(1);
                return seq.multiply(seq).multiply(k).add(c);
            }

            @Override
            public TimeSeriesConstraint transformTimeSeriesConstraint(TimeSeriesConstraint timeSeriesConstraint,
                                                                      List<BigDecimal> args) {
                assert args.size() == 2;
                BigDecimal k = args.get(0);
                BigDecimal c = args.get(1);
                // 暂时仅支持第一象限数值运算
                assert k.compareTo(BigDecimal.ZERO) > 0;

                // 不等点转换
                TimeSeriesConstraint transConstraintRes = new TimeSeriesConstraint(GlobalConstant.BASE_TIME_SERIES_NAME);
                List<BigDecimal> transNotEqualValues = new ArrayList<>();
                timeSeriesConstraint.getNotEqualValues().forEach(notEqualValue -> {
                    BigDecimal subVal = notEqualValue.subtract(c).divide(k,
                            DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP);
                    if (subVal.compareTo(BigDecimal.ZERO) < 0) return;
                    BigDecimal sqrtVal = BigDecimalUtil.sqrt(subVal, DoubleArithmeticPrecisionConstant.scale);
                    transNotEqualValues.add(sqrtVal);
                });
                transConstraintRes.setNotEqualValues(transNotEqualValues);

                // 等值范围变换
                List<RangeConstraint> transRangeConstraint = new ArrayList<>();
                timeSeriesConstraint.getRangeConstraints().forEach(rangeConstraint -> {
                    BigDecimal v1 = rangeConstraint.getGreaterEqualValue().subtract(c)
                            .divide(k, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP);
                    BigDecimal v2 = rangeConstraint.getLessEqualValue().subtract(c)
                            .divide(k, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP);

                    BigDecimal minVal = v1.min(v2);
                    BigDecimal maxVal = v1.max(v2);
                    if (minVal.compareTo(BigDecimal.ZERO) < 0 && maxVal.compareTo(BigDecimal.ZERO) < 0) return;
                    else if (minVal.compareTo(BigDecimal.ZERO) < 0 && maxVal.compareTo(BigDecimal.ZERO) > 0) {
                        transRangeConstraint.add(new RangeConstraint(BigDecimal.ZERO, BigDecimalUtil.sqrt(maxVal,
                                DoubleArithmeticPrecisionConstant.scale)));
                    } else {
                        transRangeConstraint.add(new RangeConstraint(
                                BigDecimalUtil.sqrt(minVal, DoubleArithmeticPrecisionConstant.scale),
                                BigDecimalUtil.sqrt(maxVal, DoubleArithmeticPrecisionConstant.scale)));
                    }
                });
                transConstraintRes.setRangeConstraints(transRangeConstraint);
                return transConstraintRes;
            }

            @Override
            public ConstraintValue.TimeSeriesValue transformBaseTimeSeriesValue(
                    BigDecimal factor, BigDecimal intercept, List<BigDecimal> args) {
                // c1 = k * c0 * c0 + c 代入 k * c1 + intercept
                return null;
            }
        },
        sqrtEquation {
            // y = log(x + 1) + 1
            @Override
            public BigDecimal apply(SamplingFrequency samplingFrequency, long timestamp, List<BigDecimal> args) {
//                long startTimestamp = 1641024000000L;
//                long samplingFrequency = 5000L;
//                BigDecimal x = new BigDecimal(timestamp - startTimestamp).
//                        divide(new BigDecimal(samplingFrequency), DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP)
//                        .add(BigDecimal.ONE);
//                if (x.compareTo(BigDecimal.ZERO) < 0) return new BigDecimal(Integer.MIN_VALUE);
//                return (BigDecimal.valueOf(Math.log(x.doubleValue()))).add(BigDecimal.ONE);
                return null;
            }

            @Override
            public TimeSeriesConstraint transformTimeSeriesConstraint(TimeSeriesConstraint timeSeriesConstraint,
                                                                      List<BigDecimal> args) {
                return null;
            }

            @Override
            public ConstraintValue.TimeSeriesValue transformBaseTimeSeriesValue(
                    BigDecimal factor, BigDecimal intercept, List<BigDecimal> args) {
                return null;
            }
        };

        public abstract BigDecimal apply(SamplingFrequency samplingFrequency, long timestamp, List<BigDecimal> args);

        // 求解BASE_TIME_SERIES最终范围 -> BASE_TIME_SERIES >= 0恒成立
        public abstract TimeSeriesConstraint transformTimeSeriesConstraint(TimeSeriesConstraint timeSeriesConstraint,
                                                                           List<BigDecimal> args);

        public abstract ConstraintValue.TimeSeriesValue transformBaseTimeSeriesValue(
                BigDecimal factor, BigDecimal intercept, List<BigDecimal> args);

        private static BigDecimal timestampToBaseSeq(SamplingFrequency samplingFrequency, long timestamp) {
            return samplingFrequency.getSeqByTimestamp(timestamp);
        }
    }

    public BigDecimal genValueByTimestamp(SamplingFrequency samplingFrequency, long timestamp) {
        return this.equationType.apply(samplingFrequency, timestamp, args)
                .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP);
    }

    public TimeSeriesConstraint transformTimeSeriesConstraint(TimeSeriesConstraint timeSeriesConstraint) {
        return this.equationType.transformTimeSeriesConstraint(timeSeriesConstraint, args);
    }

    public ConstraintValue.TimeSeriesValue transformBaseTimeSeriesValue(BigDecimal factor,
                                                                        BigDecimal intercept) {
        return this.equationType.transformBaseTimeSeriesValue(factor, intercept, args);
    }

}
