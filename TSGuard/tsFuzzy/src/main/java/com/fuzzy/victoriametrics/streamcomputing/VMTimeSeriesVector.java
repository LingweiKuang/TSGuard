package com.fuzzy.victoriametrics.streamcomputing;

import com.fuzzy.common.streamprocessing.TimeSeriesVectorOperation;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesElement;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Slf4j
public class VMTimeSeriesVector extends TimeSeriesStream.TimeSeriesVector implements TimeSeriesVectorOperation {

    // 封装语义不一致的操作运算(静态代理)
    public VMTimeSeriesVector(TimeSeriesVector vector) {
        super(vector.getElements());
    }

    @Override
    public TimeSeriesVector and(TimeSeriesVector rightVector) {
        Map<String, TimeSeriesElement> reservedElements = new HashMap<>();

        Map<String, TimeSeriesElement> leftElements = this.getElements();
        leftElements.forEach((hashKey, element) -> {
            // 右侧无匹配的标签数据, 则将当前元素忽略
            if (!rightVector.getElements().containsKey(hashKey)) {
                return;
            }

            // 标签匹配成功, 进行时间戳匹配
            Map<Long, BigDecimal> leftTimestampToValues = element.getValues();
            Map<Long, BigDecimal> rightTimestampToValues = rightVector.getElements().get(hashKey).getValues();

            Map<Long, BigDecimal> filteredValues =
                    leftTimestampToValues.entrySet().stream().filter(entry -> {
                        // 仅保留右侧向量时间序列元素包含左侧时间戳的数值
                        return rightTimestampToValues.containsKey(entry.getKey());
                    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // 存储保留后的数值
            if (!filteredValues.isEmpty()) {
                reservedElements.put(hashKey, new TimeSeriesElement(element.getName(), element.getLabelSets(),
                        filteredValues));
            }
        });
        return new TimeSeriesVector(reservedElements);
    }

    @Override
    public TimeSeriesVector or(TimeSeriesVector rightVector) {
        // 保留左侧向量全部值
        Map<String, TimeSeriesElement> reservedElements = new HashMap<>(this.getElements());

        Map<String, TimeSeriesElement> rightElements = rightVector.getElements();
        rightElements.forEach((hashKey, element) -> {
            // 左侧不存在, 则将当前元素纳入新集合
            if (!this.getElements().containsKey(hashKey)) {
                reservedElements.put(hashKey, element);
                return;
            }

            // 比较时间戳, 筛出左侧不存在的时间戳纳入集合
            Map<Long, BigDecimal> leftValues = this.getElements().get(hashKey).getValues();
            Map<Long, BigDecimal> rightValues = element.getValues();
            Map<Long, BigDecimal> reservedValues = rightValues.entrySet().stream()
                    .filter(entry -> !leftValues.containsKey(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            // 保留右侧时序元素 时间戳集合 => name 仍为原始左侧元素的 metric name, 读取 VM 返回的数据也保持该逻辑
            if (!reservedValues.isEmpty()) {
                reservedElements.get(element.getLabelSetsHashKey()).getValues().putAll(reservedValues);
            }
        });
        return new TimeSeriesVector(reservedElements);
    }

    @Override
    public TimeSeriesVector unless(TimeSeriesVector rightVector) {
        // 左表达式筛除右表达式
        Map<String, TimeSeriesElement> reservedElements = this.getElements().entrySet().stream().map(
                        entry -> {
                            // 右侧标签匹配不成功, 直接保留
                            if (!rightVector.getElements().containsKey(entry.getKey())) return entry;

                            // 右侧标签匹配成功, 按时间戳筛除
                            TimeSeriesElement element = entry.getValue();
                            TimeSeriesElement rightElement = rightVector.getElements().get(entry.getKey());
                            Map<Long, BigDecimal> reservedValues = element.getValues().entrySet().stream()
                                    .filter(timestampToValues ->
                                            !rightElement.getValues().containsKey(timestampToValues.getKey()))
                                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                            // 剔除空值元素
                            if (reservedValues.isEmpty()) {
                                return null;
                            }
                            return Map.entry(entry.getKey(),
                                    new TimeSeriesElement(element.getName(), element.getLabelSets(), reservedValues));
                        })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new TimeSeriesVector(reservedElements);
    }

    /**
     * VM MetricsQL 和 PromQL 区别在于：MetricsQL 不返回任意 NaN 值，故忽略预期结果集中包含的 NaN 值
     * Source: MetricsQL removes all the NaN values from the output, so some queries like (-1)^0.5 return empty results in VictoriaMetrics, while returning a series of NaN values in Prometheus. Note that Grafana doesn’t draw any lines or dots for NaN values, so the end result looks the same for both VictoriaMetrics and Prometheus.
     */
    @Override
    public boolean containsAndEquals(TimeSeriesStream expectedResultSet) {
        if (expectedResultSet == null) {
            return this.getElements().isEmpty();
        } else if (expectedResultSet instanceof TimeSeriesScalar scalar) {
            // 真实结果集为向量, 预期结果集为标量
            // metric 和 label 均为空, 且值内包含标量元素
            return this.getElements().size() == 1 && this.getElements().containsKey("")
                    && this.getElements().get("").containsScalarValue(scalar.getScalarValue());
        } else if (expectedResultSet instanceof TimeSeriesVector expectedVector) {
            // 时间序列数目不相同 -> 右侧可能存在 NAN 值
            if (this.getElements().size() != expectedVector.getElements().size()) {
                // 如果右侧元素均为 NaN, 则忽略本次比较
                boolean allElementIsValue = expectedVector.getElements().values().stream()
                        .allMatch(TimeSeriesElement::allValueIsNaN);
                if (allElementIsValue) return true;

                log.error("this.elements:{} expectedVector.elements:{} this.size:{} expectedVector.size:{}",
                        this.getElements().keySet(), expectedVector.getElements().keySet(),
                        this.getElements().size(), expectedVector.getElements().size());
                return false;
            }
            boolean res = true;
            for (String elementHashKey : expectedVector.getElements().keySet()) {
                TimeSeriesElement expectedElement = expectedVector.getElements().get(elementHashKey);
                if (!this.getElements().containsKey(elementHashKey)) {
                    // 如果右侧元素均为 NaN, 则忽略本次比较
                    if (expectedElement.allValueIsNaN()) continue;

                    log.error("真实时间序列不包含预期时间序列, expected time series: {}", elementHashKey);
                    res = false;
                    break;
                }
                // 判断每条时序向量是否满足 包含关系
                res &= this.getElements().get(elementHashKey).containsElementIgnoreNaN(
                        expectedElement);
            }
            return res;
        } else {
            throw new UnsupportedOperationException("Unsupported stream type");
        }
    }

    /**
     * Comparison binary operators
     * VM MetricsQL 和 PromQL 区别在于：比较运算符返回的向量序列可以为 NaN 元素，再次数据操作时会将 NaN 作为新元素，而 PromQL 会 drop 掉不符合规范的元素
     * Source:
     *
     * @param other
     * @return
     */
    @Override
    public TimeSeriesStream comparisonBinaryOperation(TimeSeriesStream other, BiFunction<BigDecimal, BigDecimal, BigDecimal> op) {
        // TODO 仅不等号使用当前 comparisonBinaryOperation
        // 获取满足不等号判定的运算组合，即破除 ==, <=, >=, <, > 限制，即取三组数使得组合满足的同时，能够说明 op 非其他比较运算符
        // ==: 不满足 op.apply(BigDecimal.ONE, BigDecimal.ONE).compareTo(BigDecimal.ZERO) == 0
        // >, >=: 不满足 op.apply(BigDecimal.ONE, BigDecimal.TEN).compareTo(BigDecimal.ONE) == 0
        // <, <=: 不满足 op.apply(BigDecimal.TEN, BigDecimal.ONE).compareTo(BigDecimal.ONE) == 0
        if (!(op.apply(BigDecimal.ONE, BigDecimal.ONE).compareTo(BigDecimal.ZERO) == 0
                && op.apply(BigDecimal.ONE, BigDecimal.TEN).compareTo(BigDecimal.ONE) == 0
                && op.apply(BigDecimal.TEN, BigDecimal.ONE).compareTo(BigDecimal.ONE) == 0)) {
            return super.comparisonBinaryOperation(other, op);
        }
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
                // TODO 右侧为空值的情况下, 直接返回左值
                if (rightTimeSeriesVector.getElements().isEmpty()) {
                    updatedElements.put(hashKey, new TimeSeriesElement(element.getName(),
                            new HashMap<>(element.getLabelSets()), element.getValues()));
                    return;
                } else if (!rightTimeSeriesVector.getElements().containsKey(hashKey)) {
                    // 右侧找不到匹配的标签值, 忽略该元素
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
}
