package com.fuzzy.prometheus.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.TSAFDataType;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.prometheus.PrometheusGlobalState;
import com.fuzzy.prometheus.PrometheusSchema;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusTable;
import com.fuzzy.prometheus.apiEntry.PrometheusInsertParam;
import com.fuzzy.prometheus.apiEntry.entity.CollectorAttribute;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrometheusInsertGenerator {

    private final PrometheusTable table;
    private final ExpectedErrors errors = new ExpectedErrors();
    private final PrometheusGlobalState globalState;

    // database_table -> randomGenTime
    private static Map<String, Boolean> isRandomlyGenerateTimestamp = new HashMap<>();
    // database_table -> lastTimestamp
    private static Map<String, Long> lastTimestamp = new HashMap<>();

    public PrometheusInsertGenerator(PrometheusGlobalState globalState, PrometheusTable table) {
        this.globalState = globalState;
        this.table = table;
        String hashKey = generateHashKey(globalState.getDatabaseName(), table.getName());
        if (!isRandomlyGenerateTimestamp.containsKey(hashKey)) {
            if (globalState.usesTSAF() || globalState.usesStreamComputing())
                isRandomlyGenerateTimestamp.put(hashKey, false);
            else isRandomlyGenerateTimestamp.put(hashKey, Randomly.getBoolean());
            lastTimestamp.put(hashKey, globalState.getOptions().getStartTimestampOfTSData());
        }
    }

    public static SQLQueryAdapter insertRow(PrometheusGlobalState globalState) throws SQLException {
        PrometheusTable table = globalState.getSchema().getRandomTable();
        return insertRow(globalState, table);
    }

    public static SQLQueryAdapter insertRow(PrometheusGlobalState globalState, PrometheusTable table) throws SQLException {
        return new PrometheusInsertGenerator(globalState, table).generateInsert();
    }

    private SQLQueryAdapter generateInsert() {
        PrometheusInsertParam insertParam = new PrometheusInsertParam();

        // 产出 Prometheus 数据
        if (globalState.usesTSAF() || globalState.usesStreamComputing()) {
            insertParam.setCollectorMap(generateInsertForTSAF());
        } else {

        }

        return new SQLQueryAdapter(insertParam.genPrometheusQueryParam(), errors);
    }

    private Map<String, CollectorAttribute> generateInsertForTSAF() {
        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        String databaseAndTableName = generateHashKey(databaseName, tableName);
        int nrRows = (int) PrometheusTableGenerator.SAMPLING_NUMBER;
        long startTimestamp = globalState.getNextSampleTimestamp(lastTimestamp.get(databaseAndTableName));
        long endTimestamp = startTimestamp + nrRows * globalState.getOptions().getSamplingFrequency();
        SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection(databaseName, tableName);
        List<Long> timestamps = samplingFrequency.apply(startTimestamp, endTimestamp);
        lastTimestamp.put(databaseAndTableName, endTimestamp);
        // 仅支持插入最新数据进行覆盖, 将以历史数据作为行数据
        // MetricName -> CollectorAttribute
        Map<String, CollectorAttribute> collectorMap = new HashMap<>();

        // 每列均需要按照采样间隔插入全部数据, 未插入数据点的地方通过空值进行标注
        for (PrometheusSchema.PrometheusColumn column : table.getColumns()) {
            String columnName = column.getName();
            // random generate double val
            List<Double> doubles = new ArrayList<>();
            for (int row = 0; row < nrRows; row++) {
                // TODO TSAFDataType.INT
                BigDecimal nextValue = EquationsManager.getInstance()
                        .initEquationsFromTimeSeries(databaseName, tableName, columnName, TSAFDataType.INT)
                        .genValueByTimestamp(samplingFrequency, timestamps.get(row));
                // TODO NULL VALUE
                doubles.add(nextValue.doubleValue());
            }

            CollectorAttribute attribute = new CollectorAttribute();
            attribute.setDataType(column.getType());
            attribute.setMetricName(databaseName);
            attribute.setTableName(tableName);
            attribute.setTimeSeriesName(columnName);
            attribute.setDoubleValues(doubles);
            attribute.setTimestamps(timestamps);
            attribute.setHelp(String.format("%s.%s.%s", databaseName, tableName, columnName));
            collectorMap.put(attribute.getUniqueHashKey(), attribute);
        }
        return collectorMap;
    }

    public static Long getLastTimestamp(String databaseName, String tableName) {
        return lastTimestamp.get(generateHashKey(databaseName, tableName));
    }

    public static String generateHashKey(String databaseName, String tableName) {
        return String.format("%s_%s", databaseName, tableName);
    }
}
