package com.fuzzy.common.streamprocessing;

import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesElement;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.common.tsaf.Equations;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamGeneration {

    /**
     * 通过采样得到原始 vector
     *
     * @return TimeSeriesVector
     */
    public static TimeSeriesStream.TimeSeriesVector genVectorFromSampling(String databaseName, String tableName,
                                                                          List<String> fetchColumnNames,
                                                                          long startTimestamp, long endTimestamp,
                                                                          BigDecimal tolerance) {
        SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection(databaseName, tableName);
        // 获取原始向量时间戳集合
        List<Long> timestamps = samplingFrequency.apply(startTimestamp, endTimestamp);

        // 依据向量中时序不同(采样函数不一致), 生成对应采样值
        List<TimeSeriesElement> elements = new ArrayList<>();
        fetchColumnNames.forEach(columnName -> {
            Map<Long, BigDecimal> timestampToValueMap = new HashMap<>();
            Equations equation = EquationsManager.getInstance().getEquationsFromTimeSeries(databaseName, tableName, columnName);
            for (Long timestamp : timestamps) {
                BigDecimal value = equation.genValueByTimestamp(samplingFrequency, timestamp);
                timestampToValueMap.put(timestamp, value);
            }
            // element
            elements.add(new TimeSeriesElement(databaseName, new HashMap<>() {{
                put(TimeSeriesLabelConstant.TABLE.getLabel(), tableName);
                put(TimeSeriesLabelConstant.TIME_SERIES.getLabel(), columnName);
            }}, timestampToValueMap));
        });
        return new TimeSeriesStream.TimeSeriesVector(elements);
    }

}
