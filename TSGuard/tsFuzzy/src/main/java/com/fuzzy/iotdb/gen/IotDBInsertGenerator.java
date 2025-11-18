package com.fuzzy.iotdb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.schema.AbstractTableColumn;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.TableToNullValuesManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.iotdb.IotDBErrors;
import com.fuzzy.iotdb.IotDBGlobalState;
import com.fuzzy.iotdb.IotDBSchema;
import com.fuzzy.iotdb.IotDBSchema.IotDBColumn;
import com.fuzzy.iotdb.IotDBSchema.IotDBTable;
import com.fuzzy.iotdb.IotDBVisitor;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IotDBInsertGenerator {

    private final IotDBTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final IotDBGlobalState globalState;
    // database_table -> randomGenTime
    private static Map<String, Boolean> isRandomlyGenerateTimestamp = new HashMap<>();
    // database_table -> lastTimestamp
    private static Map<String, Long> lastTimestamp = new HashMap<>();

    public IotDBInsertGenerator(IotDBGlobalState globalState, IotDBTable table) {
        this.globalState = globalState;
        this.table = table;
        String hashKey = generateHashKey(globalState.getDatabaseName(), table.getName());
        if (!isRandomlyGenerateTimestamp.containsKey(hashKey)) {
            if (globalState.usesTSAF()) isRandomlyGenerateTimestamp.put(hashKey, false);
            else isRandomlyGenerateTimestamp.put(hashKey, Randomly.getBoolean());
            lastTimestamp.put(hashKey, globalState.getOptions().getStartTimestampOfTSData());
        }
    }

    public static SQLQueryAdapter insertRow(IotDBGlobalState globalState) throws SQLException {
        IotDBTable table = globalState.getSchema().getRandomTable();
        return insertRow(globalState, table);
    }

    public static SQLQueryAdapter insertRow(IotDBGlobalState globalState, IotDBTable table) throws SQLException {
        return new IotDBInsertGenerator(globalState, table).generateInsert();
    }

    private SQLQueryAdapter generateInsert() {
        sb.append("INSERT INTO ");
        sb.append(table.getDatabaseName()).append(".").append(table.getName());

        if (globalState.usesTSAF()) generateInsertForTSAF();
        else generateRandomInsert();

        IotDBErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, false);
    }

    private void generateInsertForTSAF() {
        sb.append("(")
                .append("timestamp, ");
        List<IotDBColumn> columns = table.getColumns();
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(")").append(" VALUES ");

        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        String databaseAndTableName = generateHashKey(databaseName, tableName);
        long startTimestamp = globalState.getNextSampleTimestamp(lastTimestamp.get(databaseAndTableName));
        long endTimestamp = startTimestamp + IotDBTableGenerator.SAMPLING_NUMBER
                * globalState.getOptions().getSamplingFrequency();
        SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection(databaseName, tableName);
        List<Long> timestamps = samplingFrequency.apply(startTimestamp, endTimestamp);
        lastTimestamp.put(databaseAndTableName, endTimestamp);
        for (long nextTimestamp : timestamps) {
            sb.append("(");
            // timestamp -> 按照采样间隔顺序插入(起始时间戳 -> 至今)
            sb.append(nextTimestamp);
            // columns
            boolean isNull = false;
            // 空值概率存在
            if (Randomly.getBoolean(1, 50)) {
                TableToNullValuesManager.addNullValueToTable(databaseName, tableName, nextTimestamp);
                isNull = true;
            }
            for (int c = 0; c < columns.size(); c++) {
                String columnName = columns.get(c).getName();
                IotDBSchema.IotDBDataType type = columns.get(c).getType();
                BigDecimal nextValue = EquationsManager.getInstance()
                        .initEquationsFromTimeSeries(databaseName, tableName, columnName, type.toTSAFDataType())
                        .genValueByTimestamp(samplingFrequency, nextTimestamp);
                if (isNull) {
                    sb.append(", null");
                    continue;
                }
                IotDBSchema.IotDBDataType columnType = columns.get(c).getType();
                switch (columnType) {
                    case BIGDECIMAL:
                    case FLOAT:
                    case DOUBLE:
                        sb.append(", ").append(nextValue.doubleValue());
                        break;
                    case INT32:
                    case INT64:
                        sb.append(", ").append(nextValue.longValue());
                        break;
                    case BOOLEAN:
                    case TEXT:
                    case NULL:
                        sb.append(", ").append(nextValue.toPlainString());
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            sb.append("), ");
        }
        sb.delete(sb.length() - 2, sb.length());
    }

    private void generateRandomInsert() {
        boolean hasTimestampColumn = Randomly.getBoolean();
        List<IotDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        if (hasTimestampColumn) sb.append("timestamp, ");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        IotDBExpressionGenerator gen = new IotDBExpressionGenerator(globalState);
        int nrRows;
        // TODO need timestamps when insert multi rows
        if (Randomly.getBoolean() || !hasTimestampColumn) {
            nrRows = 1;
        } else {
            nrRows = 1 + Randomly.smallNumber();
        }
        for (int row = 0; row < nrRows; row++) {
            sb.append("(");
            // IotDB timestamp 必须放在列第一位
            // TimeStamp列特殊处理, 不置于Column中
            if (hasTimestampColumn) {
                // timestamp -> 按照采样间隔顺序插入 or 随机生成(起始时间戳 -> 至今)
                String databaseAndTableName = generateHashKey(globalState.getDatabaseName(), table.getName());
                if (isRandomlyGenerateTimestamp.get(databaseAndTableName)) {
                    sb.append(globalState.getRandomTimestamp())
                            .append(", ");
                } else {
                    long nextTimestamp = globalState.getNextSampleTimestamp(lastTimestamp.get(databaseAndTableName));
                    sb.append(nextTimestamp)
                            .append(", ");
                    lastTimestamp.put(databaseAndTableName, nextTimestamp);
                }
            }
            for (int c = 0; c < columns.size(); c++) {
                sb.append(IotDBVisitor.asString(gen.generateConstantForIotDBDataType(columns.get(c).getType())))
                        .append(", ");
            }
            sb.delete(sb.length() - 2, sb.length())
                    .append(")").append(",");
        }
        sb.delete(sb.length() - 1, sb.length());
    }

    public static Long getLastTimestamp(String databaseName, String tableName) {
        return lastTimestamp.get(generateHashKey(databaseName, tableName));
    }

    public static String generateHashKey(String databaseName, String tableName) {
        return String.format("%s_%s", databaseName, tableName);
    }
}
