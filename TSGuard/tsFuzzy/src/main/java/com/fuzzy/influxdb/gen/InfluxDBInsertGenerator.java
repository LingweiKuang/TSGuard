package com.fuzzy.influxdb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.influxdb.InfluxDBErrors;
import com.fuzzy.influxdb.InfluxDBGlobalState;
import com.fuzzy.influxdb.InfluxDBSchema;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBColumn;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBTable;
import com.fuzzy.influxdb.InfluxDBVisitor;
import com.fuzzy.influxdb.ast.InfluxDBConstant;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class InfluxDBInsertGenerator {

    public static final int SAMPLING_NUMBER = 10;
    private final InfluxDBTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final InfluxDBGlobalState globalState;
    // database_table -> randomGenTime
    private static Map<String, Boolean> isRandomlyGenerateTimestamp = new HashMap<>();
    // database_table -> lastTimestamp
    private static Map<String, Long> lastTimestamp = new HashMap<>();

    public InfluxDBInsertGenerator(InfluxDBGlobalState globalState, InfluxDBTable table) {
        this.globalState = globalState;
        this.table = table;
        String hashKey = generateHashKey(globalState.getDatabaseName(), table.getName());
        if (!isRandomlyGenerateTimestamp.containsKey(hashKey)) {
            if (globalState.usesTSAF()) isRandomlyGenerateTimestamp.put(hashKey, false);
            else isRandomlyGenerateTimestamp.put(hashKey, Randomly.getBoolean());
            lastTimestamp.put(hashKey, globalState.getOptions().getStartTimestampOfTSData());
        }
    }

    public static SQLQueryAdapter insertRow(InfluxDBGlobalState globalState) {
        InfluxDBTable table = globalState.getSchema().getRandomTable();
        return insertRow(globalState, table);
    }

    public static SQLQueryAdapter insertRow(InfluxDBGlobalState globalState, InfluxDBTable table) {
        return new InfluxDBInsertGenerator(globalState, table).generateInsert();
    }

    private SQLQueryAdapter generateInsert() {
        if (globalState.usesTSAF()) generateInsertForTSAF();
        else randomGenerateInsert();

        InfluxDBErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.deleteCharAt(sb.length() - 1).toString(), errors, true);
    }

    private void generateInsertForTSAF() {
        List<InfluxDBColumn> fieldColumns = table.getFieldColumns();

        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        String databaseAndTableName = generateHashKey(databaseName, tableName);
        long startTimestamp = globalState.getNextSampleTimestamp(lastTimestamp.get(databaseAndTableName));
        long endTimestamp = startTimestamp + SAMPLING_NUMBER * globalState.getOptions().getSamplingFrequency();
        SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection(databaseName, tableName);
        List<Long> timestamps = samplingFrequency.apply(startTimestamp, endTimestamp);
        lastTimestamp.put(databaseAndTableName, endTimestamp);
        for (Long timestamp : timestamps) {
            // 针对旧Tag插入新点值
            sb.append(table.getRandomSeries()).append(" ");
            // timestamp -> 按照采样间隔顺序插入
            long nextTimestamp = timestamp;
            for (InfluxDBColumn fieldColumn : fieldColumns) {
                InfluxDBSchema.InfluxDBDataType columnType = fieldColumn.getType();
                String columnName = fieldColumn.getName();
                sb.append(columnName)
                        .append("=");
                BigDecimal columnValue = EquationsManager
                        .getInstance().initEquationsFromTimeSeries(databaseName, tableName, columnName,
                                columnType.toTSAFDataType())
                        .genValueByTimestamp(samplingFrequency, nextTimestamp);
                switch (columnType) {
                    case BOOLEAN:
                        sb.append(InfluxDBVisitor.asString(
                                InfluxDBConstant.createBoolean(columnValue.compareTo(BigDecimal.ZERO) != 0)));
                        break;
                    case FLOAT:
                    case BIGDECIMAL:
                        sb.append(InfluxDBVisitor.asString(
                                InfluxDBConstant.createDoubleConstant(columnValue.doubleValue())));
                        break;
                    case INT:
                        sb.append(InfluxDBVisitor.asString(
                                InfluxDBConstant.createIntConstant(columnValue.longValue())));
                        break;
                    case UINT:
                        sb.append(InfluxDBVisitor.asString(
                                InfluxDBConstant.createIntConstant(columnValue.longValue(), false)));
                        break;
                    case STRING:
                        sb.append(InfluxDBVisitor.asString(
                                InfluxDBConstant.createDoubleQuotesStringConstant(columnValue.toPlainString())));
                        break;
                    case TIMESTAMP:
                    default:
                        throw new UnsupportedOperationException();
                }
                sb.append(",");
            }

            sb.deleteCharAt(sb.length() - 1)
                    .append(" ")
                    .append(nextTimestamp)
                    .append("\n");
        }
    }

    public void randomGenerateInsert() {
        InfluxDBExpressionGenerator gen = new InfluxDBExpressionGenerator(globalState);
        List<InfluxDBColumn> fieldColumns = table.getFieldColumns();

        int nrRows = globalState.getRandomly().getInteger(1, 1 + Randomly.smallNumber());
        for (int row = 0; row < nrRows; row++) {
            // 针对旧Tag插入新点值
            if (Randomly.getBoolean())
                sb.append(table.getRandomSeries())
                        .append(" ");
            else {
                // 插入新Tag
                List<InfluxDBColumn> tagColumns = table.getTagColumns();
                sb.append(table.getName());
                tagColumns.forEach(tagColumn -> {
                    sb.append(",").append(tagColumn.getName()).append("=")
                            .append(InfluxDBVisitor.asString(
                                    gen.generateConstantForInfluxDBDataType(InfluxDBSchema.InfluxDBDataType.STRING)));
                });
                sb.append(" ");
            }

            for (int c = 0; c < fieldColumns.size(); c++) {
                sb.append(fieldColumns.get(c).getName())
                        .append("=")
                        .append(InfluxDBVisitor.asString(
                                gen.generateConstantForInfluxDBDataType(fieldColumns.get(c).getType())))
                        .append(",");
            }
            // timestamp -> 按照采样间隔顺序插入 or 随机生成(起始时间戳 -> 至今)
            String databaseAndTableName = generateHashKey(globalState.getDatabaseName(), table.getName());
            if (isRandomlyGenerateTimestamp.get(databaseAndTableName)) {
                sb.deleteCharAt(sb.length() - 1)
                        .append(" ")
                        .append(globalState.getRandomTimestamp())
                        .append("\n");
            } else {
                long nextTimestamp = globalState.getNextSampleTimestamp(lastTimestamp.get(databaseAndTableName));
                sb.deleteCharAt(sb.length() - 1)
                        .append(" ")
                        .append(nextTimestamp)
                        .append("\n");
                lastTimestamp.put(databaseAndTableName, nextTimestamp);
            }
        }
    }

    public static Long getLastTimestamp(String databaseName, String tableName) {
        return lastTimestamp.get(generateHashKey(databaseName, tableName));
    }

    public static Long addLastTimestamp(String databaseName, String tableName, long timestamp) {
        return lastTimestamp.put(generateHashKey(databaseName, tableName), timestamp);
    }

    public static String generateHashKey(String databaseName, String tableName) {
        return String.format("%s_%s", databaseName, tableName);
    }
}
