package com.fuzzy.prometheus.streamcomputing;

import com.fuzzy.common.streamprocessing.TimeSeriesVectorOperation;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesElement;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PrometheusTimeSeriesVector extends TimeSeriesStream.TimeSeriesVector implements TimeSeriesVectorOperation {

    // 封装语义不一致的操作运算(静态代理)
    public PrometheusTimeSeriesVector(TimeSeriesVector vector) {
        super(vector.getName(), vector.getElements());
    }

    @Override
    public TimeSeriesStream.TimeSeriesVector and(TimeSeriesVector rightVector) {
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
            TimeSeriesElement reservedElement = new TimeSeriesElement(element.getLabelSets(), filteredValues);
            reservedElements.put(hashKey, reservedElement);
        });
        return new TimeSeriesVector(this.getName(), reservedElements);
    }

    @Override
    public TimeSeriesVector or(TimeSeriesVector rightVector) {
        // 保留左侧向量全部值
        Map<String, TimeSeriesElement> reservedElements = new HashMap<>(this.getElements());

        Map<String, TimeSeriesElement> rightElements = rightVector.getElements();
        rightElements.forEach((hashKey, element) -> {
            // 右侧匹配左侧不存在, 则将当前元素纳入新集合
            if (!this.getElements().containsKey(hashKey)) {
                reservedElements.put(hashKey, element);
            }
        });
        // TODO metricName 的变化 => 暂时不考虑跨时间序列操作
        return new TimeSeriesVector(this.getName(), reservedElements);
    }

    @Override
    public TimeSeriesVector unless(TimeSeriesVector rightVector) {
        // 筛除右侧标签匹配成功的元素值
        Map<String, TimeSeriesElement> reservedElements = this.getElements().entrySet().stream().filter(
                        entry -> !rightVector.getElements().containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new TimeSeriesVector(this.getName(), reservedElements);
    }
}
