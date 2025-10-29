package com.fuzzy.common.streamprocessing.entity;

import com.fuzzy.common.util.BigDecimalUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

@Data
@Slf4j
public abstract class TimeSeriesStream {

    public static final String ADD_OPERATION = "add";
    public static final String SUBTRACT_OPERATION = "subtract";
    public static final String MULTIPLY_OPERATION = "multiply";
    public static final String DIVIDE_OPERATION = "divide";
    public static final String ATAN2_OPERATION = "atan2";
    public static final int COMPARISON_PRECISION = 13;
    public static final int ARITHMETIC_PRECISION = 50;
    public static final BigDecimal NAN_BIGDECIMAL = new BigDecimal("9999999999998");
    public static final BigDecimal INF_BIGDECIMAL = new BigDecimal("9999999999999");
    public static final BigDecimal NEGATE_INF_BIGDECIMAL = new BigDecimal("-9999999999999");

    public boolean isVector() {
        return false;
    }

    public boolean isScalar() {
        return false;
    }

    public static class TimeSeriesVector extends TimeSeriesStream {
        // <ElementMatchHashKey, TimeSeriesElement>
        private Map<String, TimeSeriesElement> elements = new HashMap<>();

        public TimeSeriesVector() {
        }

        public TimeSeriesVector(List<TimeSeriesElement> elements) {
            for (TimeSeriesElement element : elements) {
                this.elements.put(element.getLabelSetsHashKey(), element);
            }
        }

        public TimeSeriesVector(Map<String, TimeSeriesElement> elements) {
            this.elements = elements;
        }

        public Map<String, TimeSeriesElement> getElements() {
            return this.elements;
        }

        public void setElements(Map<String, TimeSeriesElement> elements) {
            this.elements = elements;
        }

        @Override
        public boolean isVector() {
            return true;
        }

        @Override
        public TimeSeriesStream negate() {
            Map<String, TimeSeriesElement> updatedElements = this.elements.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                        // 将每个 element 中的 value 全部取反
                        TimeSeriesElement element = entry.getValue();
                        Map<Long, BigDecimal> updatedValues = element.getValues().entrySet().stream()
                                .collect(Collectors.toMap(Map.Entry::getKey,
                                        timestampToValue -> timestampToValue.getValue().negate()));
                        return new TimeSeriesElement(element.getName(), new HashMap<>(element.getLabelSets()), updatedValues);
                    }));
            return new TimeSeriesStream.TimeSeriesVector(updatedElements);
        }

        /**
         * Arithmetic binary operators
         *
         * @param other
         * @return
         */
        @Override
        public TimeSeriesStream arithmeticBinaryOperation(TimeSeriesStream other, BinaryOperator<BigDecimal> op, String operationType) {
            if (other instanceof TimeSeriesScalar scalar) {
                // 针对每个 element 均执行二元运算操作
                Map<String, TimeSeriesElement> updatedElements = this.getElements().entrySet().stream()
                        .collect(Collectors.toMap(entry -> {
                            return entry.getValue().getLabelSetsHashKey();
                        }, entry -> {
                            // 针对每个 element 内部的值元素进行 op 操作
                            Map<Long, BigDecimal> updatedValues = entry.getValue().getValues().entrySet().stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey,
                                            timestampToValues -> {
                                                return ieee754Float64ArithmeticBinaryOperation(timestampToValues.getValue(),
                                                        scalar.getScalarValue(), op, operationType);
                                            }));
                            return new TimeSeriesElement("",
                                    new HashMap<>(entry.getValue().getLabelSets()), updatedValues);
                        }));
                return new TimeSeriesVector(updatedElements);
            } else if (other instanceof TimeSeriesVector rightTimeSeriesVector) {
                // 保留左侧全部元素, 若右侧无法匹配 value 值, 则丢弃, 成功匹配则相互操作
                Map<String, TimeSeriesElement> updatedElements = new HashMap<>();
                this.getElements().forEach((hashKey, element) -> {
                    // 右侧找不到匹配的标签值, 忽略该元素
                    if (!rightTimeSeriesVector.getElements().containsKey(hashKey)) {
                        return;
                    }

                    Map<Long, BigDecimal> leftTimestampToValues = element.getValues();
                    Map<Long, BigDecimal> rightTimestampToValues = rightTimeSeriesVector.getElements().get(hashKey).getValues();
                    // 处理右侧存在对应时间戳的 value 值, 进行 op
                    Map<Long, BigDecimal> streamComputeValues = new HashMap<>();
                    leftTimestampToValues.forEach((timestamp, value) -> {
                        if (rightTimestampToValues.containsKey(timestamp)) {
                            streamComputeValues.put(timestamp, ieee754Float64ArithmeticBinaryOperation(value,
                                    rightTimestampToValues.get(timestamp), op, operationType));
                        }
                    });
                    // 值集合非空的情况, 将 element 纳入
                    if (!streamComputeValues.isEmpty()) {
                        TimeSeriesElement updatedElement = new TimeSeriesElement("",
                                new HashMap<>(element.getLabelSets()), streamComputeValues);
                        updatedElements.put(updatedElement.getLabelSetsHashKey(), updatedElement);
                    }
                });
                return new TimeSeriesVector(updatedElements);
            } else {
                throw new UnsupportedOperationException("Unsupported stream type");
            }
        }

        /**
         * Comparison binary operators
         *
         * @param other
         * @return
         */
        @Override
        public TimeSeriesStream comparisonBinaryOperation(TimeSeriesStream other, BiFunction<BigDecimal, BigDecimal, BigDecimal> op) {
            if (other instanceof TimeSeriesScalar scalar) {
                // 针对每个 element 均执行二元比较操作
                Map<String, TimeSeriesElement> updatedElements = this.getElements().entrySet().stream()
                        .map(entry -> {
                            // 针对每个 element 内部的值元素进行 op 操作 => 满足条件保留
                            TimeSeriesElement element = entry.getValue();
                            Map<Long, BigDecimal> updatedValues = element.getValues().entrySet().stream()
                                    .filter(timestampToValues -> {
                                        BigDecimal apply = op.apply(timestampToValues.getValue(), scalar.getScalarValue());
                                        return apply.compareTo(BigDecimal.ONE) == 0;
                                    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                            // 过滤掉 updatedValues 为空的时间序列元素
                            if (updatedValues.isEmpty()) {
                                return null;
                            }

                            return Map.entry(entry.getKey(), new TimeSeriesElement(element.getName(),
                                    new HashMap<>(element.getLabelSets()), updatedValues));
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return new TimeSeriesVector(updatedElements);
            } else if (other instanceof TimeSeriesVector rightTimeSeriesVector) {
                // 保留左侧全部元素, 若右侧无法匹配 value 值, 则丢弃, 成功匹配则相互操作
                Map<String, TimeSeriesElement> updatedElements = new HashMap<>();
                this.getElements().forEach((hashKey, element) -> {
                    // 右侧找不到匹配的标签值, 忽略该元素
                    if (!rightTimeSeriesVector.getElements().containsKey(hashKey)) {
                        return;
                    }

                    Map<Long, BigDecimal> leftTimestampToValues = element.getValues();
                    Map<Long, BigDecimal> rightTimestampToValues = rightTimeSeriesVector.getElements().get(hashKey).getValues();
                    // 处理右侧存在对应时间戳的 value 值, 进行 op 比较, 晒出满足条件的值
                    Map<Long, BigDecimal> streamComputeValues = new HashMap<>();
                    leftTimestampToValues.forEach((timestamp, value) -> {
                        if (rightTimestampToValues.containsKey(timestamp)) {
                            BigDecimal apply = op.apply(value, rightTimestampToValues.get(timestamp));
                            if (apply.compareTo(BigDecimal.ONE) == 0) {
                                streamComputeValues.put(timestamp, value);
                            }
                        }
                    });

                    // 存在计算值
                    if (!streamComputeValues.isEmpty()) {
                        updatedElements.put(hashKey, new TimeSeriesElement(element.getName(),
                                new HashMap<>(element.getLabelSets()), streamComputeValues));
                    }
                });
                return new TimeSeriesVector(updatedElements);
            } else {
                throw new UnsupportedOperationException("Unsupported stream type");
            }
        }

        /**
         * Verify whether the actual result set contains and equals the expected result set.
         *
         * @param expectedResultSet
         * @return
         */
        @Override
        public boolean containsAndEquals(TimeSeriesStream expectedResultSet) {
            if (expectedResultSet == null) {
                return this.elements.isEmpty();
            } else if (expectedResultSet instanceof TimeSeriesScalar scalar) {
                // 真实结果集为向量, 预期结果集为标量
                // metric 和 label 均为空, 且值内包含标量元素
                return this.getElements().size() == 1 && this.getElements().containsKey("")
                        && this.getElements().get("").containsScalarValue(scalar.getScalarValue());
            } else if (expectedResultSet instanceof TimeSeriesVector vector) {
                // 时间序列数目相同
                if (this.elements.size() != vector.getElements().size()) {
                    log.error("this.elements:{} vector.elements:{} this.size:{} vector.size:{}", this.elements.keySet(),
                            vector.getElements().keySet(), this.elements.size(), vector.getElements().size());
                    return false;
                }
                boolean res = true;
                for (String elementHashKey : vector.getElements().keySet()) {
                    if (!this.elements.containsKey(elementHashKey)) {
                        log.error("真实时间序列不包含预期时间序列, expected time series: {}", elementHashKey);
                        res = false;
                        break;
                    }
                    // 判断每条时序向量是否满足 包含关系
                    res &= this.elements.get(elementHashKey).containsElement(vector.getElements().get(elementHashKey));
                }
                return res;
            } else {
                throw new UnsupportedOperationException("Unsupported stream type");
            }
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            elements.forEach((hashKey, element) -> {
                builder.append(hashKey).append(" ");
                builder.append(new TreeMap<>(element.getValues())).append("\n");
            });
            return builder.toString();
        }
    }

    public static class TimeSeriesScalar extends TimeSeriesStream {
        private final BigDecimal scalarValue;

        public TimeSeriesScalar(BigDecimal scalarValue) {
            this.scalarValue = scalarValue;
        }

        public BigDecimal getScalarValue() {
            return this.scalarValue;
        }

        @Override
        public boolean isScalar() {
            return true;
        }

        @Override
        public TimeSeriesStream negate() {
            BigDecimal newValue = this.scalarValue.negate();
            return new TimeSeriesScalar(newValue);
        }

        /**
         * Arithmetic binary operators
         *
         * @param other
         * @return
         */
        public TimeSeriesStream arithmeticBinaryOperation(TimeSeriesStream other, BinaryOperator<BigDecimal> op, String operationType) {
            if (other instanceof TimeSeriesScalar scalar) {
                return new TimeSeriesStream.TimeSeriesScalar(ieee754Float64ArithmeticBinaryOperation(this.scalarValue,
                        scalar.getScalarValue(), op, operationType));
            } else if (other instanceof TimeSeriesVector timeSeriesVector) {
                // 针对每个 element 均执行二元运算操作
                Map<String, TimeSeriesElement> updatedElements = timeSeriesVector.getElements().entrySet().stream()
                        .collect(Collectors.toMap(entry -> {
                            // 二元算数运算剔除 metric name
                            return entry.getValue().getLabelSetsHashKey();
                        }, entry -> {
                            // 针对每个 element 内部的值元素进行 op 操作
                            Map<Long, BigDecimal> updatedValues = entry.getValue().getValues().entrySet().stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey,
                                            timestampToValues -> {
                                                return ieee754Float64ArithmeticBinaryOperation(this.scalarValue,
                                                        timestampToValues.getValue(), op, operationType);
                                            }));
                            return new TimeSeriesElement("", new HashMap<>(entry.getValue().getLabelSets()), updatedValues);
                        }));
                return new TimeSeriesVector(updatedElements);
            } else {
                throw new UnsupportedOperationException("Unsupported stream type");
            }
        }

        /**
         * Comparison binary operators
         *
         * @param other
         * @return
         */
        @Override
        public TimeSeriesStream comparisonBinaryOperation(TimeSeriesStream other, BiFunction<BigDecimal, BigDecimal, BigDecimal> op) {
            if (other instanceof TimeSeriesScalar scalar) {
                return new TimeSeriesStream.TimeSeriesScalar(op.apply(this.scalarValue, scalar.getScalarValue()));
            } else if (other instanceof TimeSeriesVector timeSeriesVector) {
                // 针对每个 element 均执行二元比较操作
                Map<String, TimeSeriesElement> updatedElements = timeSeriesVector.getElements().entrySet().stream()
                        .map(entry -> {
                            // 针对每个 entry 内部的值元素进行 op 操作 => 满足条件保留
                            TimeSeriesElement element = entry.getValue();
                            Map<Long, BigDecimal> updatedValues = element.getValues().entrySet().stream()
                                    .filter(timestampToValues -> {
                                        BigDecimal apply = op.apply(this.scalarValue, timestampToValues.getValue());
                                        return apply.compareTo(BigDecimal.ONE) == 0;
                                    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                            if (updatedValues.isEmpty()) {
                                return null;
                            }

                            return Map.entry(entry.getKey(), new TimeSeriesElement(element.getName(),
                                    new HashMap<>(element.getLabelSets()), updatedValues));
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return new TimeSeriesVector(updatedElements);
            } else {
                throw new UnsupportedOperationException("Unsupported stream type");
            }
        }

        /**
         * Verify whether the actual result set contains and equals the expected result set.
         *
         * @param expectedResultSet
         * @return
         */
        @Override
        public boolean containsAndEquals(TimeSeriesStream expectedResultSet) {
            // TODO 应该不存在解析值为标量的情况, 所有更新暂时忽略该函数
            throw new UnsupportedOperationException("Unsupported stream type: left expression is scalar");
//            if (expectedResultSet == null) {
//                return this.scalarValue == null;
//            } else if (expectedResultSet instanceof TimeSeriesStream.TimeSeriesScalar scalar) {
//                return this.scalarValue.compareTo(scalar.getScalarValue()) == 0;
//            } else if (expectedResultSet instanceof TimeSeriesStream.TimeSeriesVector timeSeriesVector) {
//                return false;
//            } else {
//                throw new UnsupportedOperationException("Unsupported stream type");
//            }
        }

        @Override
        public String toString() {
            return String.format("scalar: %s", this.scalarValue);
        }
    }

    // 方法集合
    public abstract TimeSeriesStream negate();

    public abstract TimeSeriesStream arithmeticBinaryOperation(TimeSeriesStream other, BinaryOperator<BigDecimal> op, String operationType);

    public abstract TimeSeriesStream comparisonBinaryOperation(TimeSeriesStream other, BiFunction<BigDecimal, BigDecimal, BigDecimal> op);

    public abstract boolean containsAndEquals(TimeSeriesStream expectedResultSet);

    public TimeSeriesStream add(TimeSeriesStream other) {
        return arithmeticBinaryOperation(other, BigDecimal::add, ADD_OPERATION);
    }

    public TimeSeriesStream subtract(TimeSeriesStream other) {
        return arithmeticBinaryOperation(other, BigDecimal::subtract, SUBTRACT_OPERATION);
    }

    public TimeSeriesStream multiply(TimeSeriesStream other) {
        return arithmeticBinaryOperation(other, BigDecimal::multiply, MULTIPLY_OPERATION);
    }

    public TimeSeriesStream divide(TimeSeriesStream other) {
        return arithmeticBinaryOperation(other, (l, r) ->
                l.divide(r, TimeSeriesStream.ARITHMETIC_PRECISION, RoundingMode.HALF_UP), DIVIDE_OPERATION);
    }

    public TimeSeriesStream atan2(TimeSeriesStream other) {
        return arithmeticBinaryOperation(other, (l, r) ->
                BigDecimalUtil.atan2(l, r, TimeSeriesStream.ARITHMETIC_PRECISION), ATAN2_OPERATION);
    }

    public TimeSeriesStream equal(TimeSeriesStream other) {
        return comparisonBinaryOperation(other, (l, r) ->
                l.compareTo(r) == 0 ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    public TimeSeriesStream notEqual(TimeSeriesStream other) {
        return comparisonBinaryOperation(other, (l, r) ->
                l.compareTo(r) != 0 ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    public TimeSeriesStream greaterThan(TimeSeriesStream other) {
        return comparisonBinaryOperation(other, (l, r) ->
                l.compareTo(r) > 0 ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    public TimeSeriesStream greaterOrEqual(TimeSeriesStream other) {
        return comparisonBinaryOperation(other, (l, r) ->
                l.compareTo(r) >= 0 ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    public TimeSeriesStream lessThan(TimeSeriesStream other) {
        return comparisonBinaryOperation(other, (l, r) ->
                l.compareTo(r) < 0 ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    public TimeSeriesStream lessOrEqual(TimeSeriesStream other) {
        return comparisonBinaryOperation(other, (l, r) ->
                l.compareTo(r) <= 0 ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    public BigDecimal ieee754Float64ArithmeticBinaryOperation(BigDecimal firstValue, BigDecimal secondValue,
                                                              BinaryOperator<BigDecimal> op, String operationType) {
        // 若存在 INF、NAN、-INF 则转换为 Double 进行相关计算
        if (firstValue.compareTo(NAN_BIGDECIMAL) == 0 || firstValue.compareTo(INF_BIGDECIMAL) == 0
                || firstValue.compareTo(NEGATE_INF_BIGDECIMAL) == 0 || secondValue.compareTo(NAN_BIGDECIMAL) == 0
                || secondValue.compareTo(INF_BIGDECIMAL) == 0 || secondValue.compareTo(NEGATE_INF_BIGDECIMAL) == 0) {

            Double firstDouble = getDoubleSpecialValue(firstValue);
            Double secondDouble = getDoubleSpecialValue(secondValue);
            Double res = null;
            switch (operationType) {
                case ADD_OPERATION:
                    res = firstDouble + secondDouble;
                    break;
                case SUBTRACT_OPERATION:
                    res = firstDouble - secondDouble;
                    break;
                case MULTIPLY_OPERATION:
                    res = firstDouble * secondDouble;
                    break;
                case DIVIDE_OPERATION:
                    res = firstDouble / secondDouble;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported operation type: " + operationType);
            }

            // 依据 Double 值返回 BigDecimal 标记值
            if (Double.isNaN(res)) {
                return NAN_BIGDECIMAL;
            } else if (Double.POSITIVE_INFINITY == res) {
                return INF_BIGDECIMAL;
            } else if (Double.NEGATIVE_INFINITY == res) {
                return NEGATE_INF_BIGDECIMAL;
            } else {
                return BigDecimal.valueOf(res);
            }
        } else if (DIVIDE_OPERATION.equals(operationType) && secondValue.compareTo(BigDecimal.ZERO) == 0) {
            // 除数为 0
            return firstValue.compareTo(BigDecimal.ZERO) == 0 ? NAN_BIGDECIMAL
                    : (firstValue.compareTo(BigDecimal.ZERO) > 0 ? INF_BIGDECIMAL : NEGATE_INF_BIGDECIMAL);
        }
        // 常规 BigDecimal 运算
        return op.apply(firstValue, secondValue);
    }

    private Double getDoubleSpecialValue(BigDecimal bigDecimal) {
        return bigDecimal.compareTo(NAN_BIGDECIMAL) == 0 ? Double.NaN :
                (bigDecimal.compareTo(INF_BIGDECIMAL) == 0 ? Double.POSITIVE_INFINITY :
                        (bigDecimal.compareTo(NEGATE_INF_BIGDECIMAL) == 0 ? Double.NEGATIVE_INFINITY : bigDecimal.doubleValue()));
    }
    // 运算规则置于具体数据库下面(待定), 毕竟各个数据库语义解析不同
}
