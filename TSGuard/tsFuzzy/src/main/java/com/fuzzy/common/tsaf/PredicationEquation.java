package com.fuzzy.common.tsaf;

import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Slf4j
public class PredicationEquation {
    // 依据时序约束计算符合条件的预期结果集
    // 多列筛选
    private TimeSeriesConstraint timeSeriesConstraint;
    private String equationName;

    public PredicationEquation(TimeSeriesConstraint timeSeriesConstraint, String equationName) {
        this.timeSeriesConstraint = timeSeriesConstraint;
        this.equationName = Equations.EquationType.linearEquation.name();
    }

    public Map<Long, List<BigDecimal>> genExpectedResultSet(String databaseName, String tableName,
                                                            List<String> fetchColumnNames,
                                                            long startTimestamp, long endTimestamp,
                                                            BigDecimal tolerance) {
        Map<Long, List<BigDecimal>> timestampToValues = new HashMap<>();
        SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection(databaseName, tableName);
        List<Long> timestamps = samplingFrequency.apply(startTimestamp, endTimestamp);
        for (Long timestamp : timestamps) {
            // 最后返回的范围是针对基序列而言的
            if (!timeSeriesConstraint.valueInRange(EquationsManager.getInstance()
                    .getEquationsFromTimeSeries(databaseName, tableName, GlobalConstant.BASE_TIME_SERIES_NAME)
                    .genValueByTimestamp(samplingFrequency, timestamp), tolerance))
                continue;

            List<BigDecimal> values = new ArrayList<>();
            for (String fetchColumnName : fetchColumnNames) {
                BigDecimal value = EquationsManager.getInstance().getEquationsFromTimeSeries(
                        databaseName, tableName, fetchColumnName).genValueByTimestamp(samplingFrequency, timestamp);
                values.add(value);
            }
            timestampToValues.put(timestamp, values);
        }
        return timestampToValues;
    }

}
