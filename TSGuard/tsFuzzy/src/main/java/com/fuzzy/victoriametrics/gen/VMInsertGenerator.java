package com.fuzzy.victoriametrics.gen;


import com.alibaba.fastjson.JSONObject;
import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.TSAFDataType;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.victoriametrics.VMErrors;
import com.fuzzy.victoriametrics.VMGlobalState;
import com.fuzzy.victoriametrics.VMSchema;
import com.fuzzy.victoriametrics.apientry.VMRequestParam;
import com.fuzzy.victoriametrics.apientry.VMRequestType;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class VMInsertGenerator {

    public static final int SAMPLING_NUMBER = 10;
    private final VMSchema.VMTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final VMGlobalState globalState;
    // database_table -> randomGenTime
    private static Map<String, Boolean> isRandomlyGenerateTimestamp = new HashMap<>();
    // database_table -> lastTimestamp
    private static Map<String, Long> lastTimestamp = new HashMap<>();

    public VMInsertGenerator(VMGlobalState globalState, VMSchema.VMTable table) {
        this.globalState = globalState;
        this.table = table;
        String hashKey = generateHashKey(globalState.getDatabaseName(), table.getName());
        if (!isRandomlyGenerateTimestamp.containsKey(hashKey)) {
            if (globalState.usesStreamComputing()) isRandomlyGenerateTimestamp.put(hashKey, false);
            else isRandomlyGenerateTimestamp.put(hashKey, Randomly.getBoolean());
            lastTimestamp.put(hashKey, globalState.getOptions().getStartTimestampOfTSData()
                    - globalState.getOptions().getSamplingFrequency());
        }
    }

    public static SQLQueryAdapter insertRow(VMGlobalState globalState) {
        VMSchema.VMTable table = globalState.getSchema().getRandomTable();
        return insertRow(globalState, table);
    }

    public static SQLQueryAdapter insertRow(VMGlobalState globalState, VMSchema.VMTable table) {
        return new VMInsertGenerator(globalState, table).generateInsert();
    }

    private SQLQueryAdapter generateInsert() {
        randomGenerateInsert();
        VMErrors.addInsertUpdateErrors(errors);
        VMRequestParam requestParam = new VMRequestParam(VMRequestType.INSERT_DATA,
                sb.deleteCharAt(sb.length() - 1).toString());
        return new SQLQueryAdapter(JSONObject.toJSONString(requestParam), errors, true);
    }

    private void randomGenerateInsert() {
        List<VMSchema.VMColumn> fieldColumns = table.getFieldColumns();

        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        String databaseAndTableName = generateHashKey(databaseName, tableName);
        // 目前 lastTimestamp 存储的是最后一个已经插入数据库的数据点，因此当前数据点往后挪动一个采样间隔
        long startTimestamp = globalState.getNextSampleTimestamp(lastTimestamp.get(databaseAndTableName))
                + globalState.getOptions().getSamplingFrequency();
        long endTimestamp = startTimestamp + (SAMPLING_NUMBER - 1) * globalState.getOptions().getSamplingFrequency();
        SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection(databaseName, tableName);
        List<Long> timestamps = samplingFrequency.applyClosedInterval(startTimestamp, endTimestamp);
        lastTimestamp.put(databaseAndTableName, endTimestamp);
        for (long nextTimestamp : timestamps) {
            // timestamp -> 按照采样间隔顺序插入
            for (VMSchema.VMColumn fieldColumn : fieldColumns) {
                // 时间序列
                sb.append(databaseName).append(",")
                        .append(TimeSeriesLabelConstant.TABLE.getLabel()).append("=").append(tableName).append(",");
                String columnName = fieldColumn.getName();
                sb.append(TimeSeriesLabelConstant.TIME_SERIES.getLabel()).append("=").append(columnName).append(" ");
                sb.append("value")
                        .append("=");
                // TODO TSAFDataType.INT
                BigDecimal nextValue = EquationsManager.getInstance()
                        .initEquationsFromTimeSeries(databaseName, tableName, columnName, TSAFDataType.INT)
                        .genValueByTimestamp(samplingFrequency, nextTimestamp);
                // TODO NULL VALUE
                sb.append(nextValue.doubleValue());
                sb.append(" ").append(nextTimestamp).append("\n");
            }
        }
    }

    public static Long getLastTimestamp(String databaseName, String tableName) {
        return lastTimestamp.get(generateHashKey(databaseName, tableName));
    }

    public static void putLastTimestamp(String databaseName, String tableName, long timestamp) {
        lastTimestamp.put(generateHashKey(databaseName, tableName), timestamp);
    }

    public static String generateHashKey(String databaseName, String tableName) {
        return String.format("%s_%s", databaseName, tableName);
    }
}
