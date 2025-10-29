package com.fuzzy.prometheus.gen;

import com.fuzzy.Randomly;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.prometheus.PrometheusGlobalState;
import com.fuzzy.prometheus.PrometheusSchema;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusDataType;
import com.fuzzy.prometheus.apiEntry.PrometheusInsertParam;
import com.fuzzy.prometheus.apiEntry.entity.CollectorAttribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrometheusTableGenerator {

    // TODO 鉴于回补数据缺陷(写入速度过慢会导致写入异常), 暂时不考虑将 SAMPLING_NUMBER 参数设置过大
    public static final long SAMPLING_NUMBER = 10;
    private final String tableName;
    private final Randomly r;
    private final List<String> columns = new ArrayList<>();
    private final PrometheusSchema schema;
    private final PrometheusGlobalState globalState;

    public PrometheusTableGenerator(PrometheusGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.r = globalState.getRandomly();
        this.schema = globalState.getSchema();
        this.globalState = globalState;

        // SamplingFrequency Setting, 即本轮数据库测试所横跨的全部时间范围及采样间隔
        // 每个周期采样数目: SAMPLING_NUMBER, 每个点采样频率均匀分布理应: globalState.getOptions().getSamplingFrequency()
        SamplingFrequencyManager.getInstance().addSamplingFrequency(globalState.getDatabaseName(),
                tableName, globalState.getOptions().getStartTimestampOfTSData(),
                SAMPLING_NUMBER * globalState.getOptions().getSamplingFrequency(), SAMPLING_NUMBER);
    }

    public static SQLQueryAdapter generate(PrometheusGlobalState globalState, String tableName) {
        return new PrometheusTableGenerator(globalState, tableName).create();
    }

    private SQLQueryAdapter create() {
        // 创建Table -> 创建若干column
        ExpectedErrors errors = new ExpectedErrors();

        // MetricName -> CollectorAttribute
        Map<String, CollectorAttribute> collectorMap = new HashMap<>();
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            PrometheusDataType dataType = PrometheusDataType.getRandom(globalState);
            String columnName = genColumn(i, dataType);

            CollectorAttribute attribute = new CollectorAttribute();
            attribute.setDataType(dataType);
            attribute.setMetricName(globalState.getDatabaseName());
            attribute.setHelp(String.format("%s.%s.%s", globalState.getDatabaseName(), tableName, columnName));
//            attribute.setDatabaseName(globalState.getDatabaseName());
            attribute.setTableName(tableName);
            attribute.setTimeSeriesName(columnName);
            attribute.randomInitValue(globalState.getOptions().getStartTimestampOfTSData());
            collectorMap.put(attribute.getUniqueHashKey(), attribute);
        }

        PrometheusInsertParam insertParam = new PrometheusInsertParam();
        insertParam.setCollectorMap(collectorMap);
        return new SQLQueryAdapter(insertParam.genPrometheusQueryParam(), errors, true);
    }

    private String genColumn(int columnId, PrometheusDataType dataType) {
        String columnName = DBMSCommon.createColumnName(columnId);
        columnName += dataType == PrometheusDataType.COUNTER ? TimeSeriesLabelConstant.END_WITH_COUNTER.getLabel()
                : TimeSeriesLabelConstant.END_WITH_GAUGE.getLabel();
        columns.add(columnName);
        return columnName;
    }

}
