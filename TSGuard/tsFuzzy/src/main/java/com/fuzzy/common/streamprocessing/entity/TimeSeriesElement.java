package com.fuzzy.common.streamprocessing.entity;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Map;
import java.util.TreeMap;

@Data
@Slf4j
public class TimeSeriesElement {
    private String name;
    private Map<String, String> labelSets;
    private Map<Long, BigDecimal> values;

    public TimeSeriesElement(String metricName, Map<String, String> labelSets, Map<Long, BigDecimal> values) {
        this.name = metricName;
        this.labelSets = labelSets;
        this.values = values;
    }

    /**
     * hashKey: metricName_labelSets
     *
     * @return
     */
    public String getLabelSetsHashKey() {
        StringBuilder hashKey = new StringBuilder();
//        hashKey.append(name);
        if (labelSets != null) {
            Map<String, String> sortedLabelSets = new TreeMap<>(labelSets);
            for (Map.Entry<String, String> entry : sortedLabelSets.entrySet()) {
                hashKey.append("_").append(entry.getKey()).append("_").append(entry.getValue());
            }
        }
        return hashKey.toString();
    }

    /**
     * 判断时间序列是否包含标量(左值包含右值)
     *
     * @param value
     * @return
     */
    public boolean containsScalarValue(BigDecimal value) {
        // 默认 float64 => 有效数字精度: 15–17 位十进制有效数字
        // 保留 scale 位有效小数
        MathContext mc = new MathContext(TimeSeriesStream.COMPARISON_PRECISION);
        BigDecimal valueRounded = value.round(mc);
        for (BigDecimal bigDecimal : this.values.values()) {
            // 消除精度影响 => 将 bigDecimal 和 value 转换为 scale 有效位数比较
            if (bigDecimal.round(mc).compareTo(valueRounded) == 0) {
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
        // 默认 float64 => 有效数字精度: 15–17 位十进制有效数字
        // 保留 scale 位有效小数
        MathContext mc = new MathContext(TimeSeriesStream.COMPARISON_PRECISION);
        for (Long expectedTimestamp : timeSeriesElement.values.keySet()) {
            // 统一预期结果集和真实结果集时间戳精度
            Long timestamp = expectedTimestamp / 1000;
            BigDecimal expectedValue = timeSeriesElement.getValues().get(expectedTimestamp);
            if (!this.values.containsKey(timestamp)) {
                // 1. 真实结果集不包含预期时间戳
                log.error("真实结果集不包含预期时间戳, timestamp: {}", timestamp);
                return false;
            } else {
                // 两者在指定时间戳不一致, 返回 false
                if (expectedValue.round(mc).compareTo(values.get(timestamp).round(mc)) != 0) {
                    log.error("真实结果集包含预期时间戳, 但是值不匹配. timestamp: {} expectedValue: {} actualValue:{}",
                            timestamp, expectedValue, values.get(timestamp));
                    return false;
                }
            }
        }
        return true;
    }
}
