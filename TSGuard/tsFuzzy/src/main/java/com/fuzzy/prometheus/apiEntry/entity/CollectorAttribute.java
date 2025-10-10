package com.fuzzy.prometheus.apiEntry.entity;

import build.buf.gen.io.prometheus.write.v2.Request;
import build.buf.gen.io.prometheus.write.v2.Sample;
import build.buf.gen.io.prometheus.write.v2.TimeSeries;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusDataType;
import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Data
public class CollectorAttribute {
    private PrometheusDataType dataType;
    private String metricName;
    private String tableName;
    private List<Double> doubleValues;
    //    private List<Integer> intValues;
    private List<Long> timestamps;
    // nullable
    private String timeSeriesName;
    private Map<String, String> labels;
    private String help;

    public CollectorAttribute() {
        this.doubleValues = new ArrayList<>();
//        this.intValues = new ArrayList<>();
        this.timestamps = new ArrayList<>();
    }

    public void randomInitValue(long startTimestamp) {
//        switch (this.dataType) {
//            case COUNTER:
//                this.doubleValues = Collections.singletonList((double) randomly.getInteger(1, Integer.MAX_VALUE));
//                break;
//            case GAUGE:
//                this.doubleValues = Collections.singletonList(randomly.getInfiniteDouble());
//                break;
//            case HISTOGRAM:
//            case SUMMARY:
//            default:
//        }
        // 初始化表时间: 测试区间左边界 startTimestamp - 100s
        this.doubleValues = Collections.singletonList(0.0);
        this.timestamps = Collections.singletonList(startTimestamp - 100 * 1000L);
    }

    public void defaultValue(long startTimestamp) {
//        switch (this.dataType) {
//            case COUNTER:
//                this.intValues = Collections.singletonList(0);
//                break;
//            case GAUGE:
//                this.doubleValues = Collections.singletonList(0.0);
//                break;
//            case HISTOGRAM:
//            case SUMMARY:
//            default:
//        }
        this.doubleValues = Collections.singletonList(0.0);
        // 初始时间戳: startTimestamp - 200s
        this.timestamps = Collections.singletonList(startTimestamp - 200 * 1000L);
    }

    public Request createRequestForRemoteWrite() {
        Request.Builder requestBuilder = Request.newBuilder();

        // 处理时序指标
        Map<String, String> labels = new HashMap<>();
        labels.put(TimeSeriesLabelConstant.METRIC_KEY.getLabel(), metricName);
        labels.put(TimeSeriesLabelConstant.TABLE.getLabel(), this.tableName);
        if (!StringUtils.isBlank(this.timeSeriesName))
            labels.put(TimeSeriesLabelConstant.TIME_SERIES.getLabel(), this.timeSeriesName);
        if (!ObjectUtils.isEmpty(this.labels))
            labels.putAll(this.labels);
        // 添加标签按照指标逐条添加至 requestBuilder 符号表（严格交替存储）
        addSymbol(requestBuilder, labels);

        // 创建 TimeSeries, 相关标签索引需准确对应符号表中位置
        TimeSeries.Builder tsBuilder = TimeSeries.newBuilder();
        List<Integer> labelRefs = IntStream.range(0, labels.size() * 2)
                .boxed()
                .collect(Collectors.toList());
        tsBuilder.addAllLabelsRefs(labelRefs);

        // 生成采样点
        List<Sample> sampleList = new ArrayList<>();
        for (int i = 0; i < this.doubleValues.size(); i++) {
            sampleList.add(Sample.newBuilder()
                    .setValue(this.doubleValues.get(i))
                    .setTimestamp(this.timestamps.get(i))
                    .build());
        }
        sampleList.sort(Comparator.comparingLong(Sample::getTimestamp));

        tsBuilder.addAllSamples(sampleList);
        return requestBuilder.addTimeseries(tsBuilder.build()).build();
    }

    private void addSymbol(Request.Builder requestBuilder, Map<String, String> labels) {
        labels.forEach((label, value) -> {
            requestBuilder.addSymbols(label);
            requestBuilder.addSymbols(value);
        });
    }

    //    public Collector transToCollector() {
//        List<String> labelValues = new ArrayList<>();
//        labelValues.add(this.tableName);
//        labelValues.addAll(labels.values());
//
//        switch (this.dataType) {
//            case COUNTER:
//                Counter counter = Counter.build()
//                        .name(this.metricName)
//                        .labelNames(PrometheusLabelConstant.TABLE.getLabel())
//                        .labelNames(labels.keySet().toArray(new String[0]))
//                        .help(this.help)
//                        .register();
//                // TODO
//                counter.labels(labelValues.toArray(new String[0])).inc(intValues.get(0));
//                return counter;
//            case GAUGE:
//                Gauge gauge = Gauge.build()
//                        .name(this.metricName)
//                        .labelNames(PrometheusLabelConstant.TABLE.getLabel())
//                        .labelNames(labels.keySet().toArray(new String[0]))
//                        .help(this.help)
//                        .register();
//                gauge.labels(labelValues.toArray(new String[0])).set(doubleValues.get(0));
//                return gauge;
//            case HISTOGRAM:
//            case SUMMARY:
//            default:
//                return null;
//        }
//    }

    /**
     * unique hash key: metricName_tableName_timeSeriesName_labelSets
     *
     * @return
     */
    public String getUniqueHashKey() {
        StringBuilder uniqueHashKey = new StringBuilder();
        uniqueHashKey.append(metricName);
        if (StringUtils.isNotBlank(tableName)) uniqueHashKey.append("_").append(tableName);
        if (StringUtils.isNotBlank(timeSeriesName)) uniqueHashKey.append("_").append(timeSeriesName);

        if (labels != null) {
            for (String labelKey : labels.keySet()) {
                if (StringUtils.isNotBlank(labelKey)) {
                    uniqueHashKey.append("_").append(labelKey).append("_").append(labels.get(labelKey));
                }
            }
        }
        return uniqueHashKey.toString();
    }
}
