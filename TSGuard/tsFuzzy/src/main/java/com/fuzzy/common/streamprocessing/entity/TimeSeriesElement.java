package com.fuzzy.common.streamprocessing.entity;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;
import java.util.TreeMap;

@Data
@Slf4j
public class TimeSeriesElement {
    private Map<String, String> labelSets;
    private Map<Long, BigDecimal> values;

    public TimeSeriesElement() {
    }

    public TimeSeriesElement(Map<String, String> labelSets, Map<Long, BigDecimal> values) {
        this.labelSets = labelSets;
        this.values = values;
    }

    /**
     * hashKey: metricName_labelSets
     *
     * @param metricName
     * @return
     */
    public String getLabelSetsHashKey(String metricName) {
        StringBuilder hashKey = new StringBuilder();
        hashKey.append(metricName);
        if (labelSets != null) {
            Map<String, String> sortedLabelSets = new TreeMap<>(labelSets);
            for (Map.Entry<String, String> entry : sortedLabelSets.entrySet()) {
                hashKey.append("_").append(entry.getKey()).append("_").append(entry.getValue());
            }
        }
        return hashKey.toString();
    }

    /**
     * 判断时间序列是否包含标量
     *
     * @param value
     * @return
     */
    public boolean containsScalarValue(BigDecimal value) {
        for (BigDecimal bigDecimal : this.values.values()) {
            if (bigDecimal.compareTo(value) == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * 比较本元素 是否包含 其他元素
     *
     * @param timeSeriesElement
     * @return
     */
    public boolean containsElement(TimeSeriesElement timeSeriesElement) {
        for (Long expectedTimestamp : timeSeriesElement.values.keySet()) {
            // 统一预期结果集和真实结果集时间戳精度
            Long timestamp = expectedTimestamp / 1000;
            BigDecimal expectedValue = timeSeriesElement.getValues().get(expectedTimestamp);
            if (!this.values.containsKey(timestamp)) {
                // 1. 真实结果集不包含预期时间戳
                log.error("真实结果集不包含预期时间戳, timestamp: {}", timestamp);
                return false;
            } /* else if (expectedValues.size() != actualResultSet.get(timestamp).size()) {
                // 2. 真是结果集在该点值数目与预期结果集不一致
                return VerifyResultState.FAIL;
            } */ else {
//                return compareSortedValues(expectedValues, actualResultSet.get(timestamp));
                // 两者在指定时间戳不一致, 返回 false
                if (expectedValue.compareTo(values.get(timestamp)) != 0) {
                    log.error("真实结果集包含预期时间戳, 但是值不匹配. timestamp: {} expectedValue: {} actualValue:{}",
                            timestamp, expectedValue, values.get(timestamp));
                    return false;
                }
            }
        }
        return true;
    }
}
