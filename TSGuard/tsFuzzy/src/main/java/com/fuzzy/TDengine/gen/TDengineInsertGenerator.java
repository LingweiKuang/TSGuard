package com.fuzzy.TDengine.gen;


import com.fuzzy.Randomly;
import com.fuzzy.TDengine.TDengineErrors;
import com.fuzzy.TDengine.TDengineGlobalState;
import com.fuzzy.TDengine.TDengineSchema;
import com.fuzzy.TDengine.TDengineSchema.TDengineColumn;
import com.fuzzy.TDengine.TDengineSchema.TDengineTable;
import com.fuzzy.TDengine.TDengineVisitor;
import com.fuzzy.TDengine.ast.TDengineCastOperation;
import com.fuzzy.TDengine.ast.TDengineConstant;
import com.fuzzy.TDengine.tsaf.enu.TDengineConstantString;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.schema.AbstractTableColumn;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.TableToNullValuesManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class TDengineInsertGenerator {

    private final TDengineTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final TDengineGlobalState globalState;
    // database_table -> randomGenTime
    private static final Map<String, Boolean> isRandomlyGenerateTimestamp = new HashMap<>();
    // database_table -> lastTimestamp
    private static final Map<String, Long> lastTimestamp = new HashMap<>();

    public TDengineInsertGenerator(TDengineGlobalState globalState, TDengineTable table) {
        this.globalState = globalState;
        this.table = table;
        String hashKey = generateHashKey(globalState.getDatabaseName(), table.getName());
        if (!isRandomlyGenerateTimestamp.containsKey(hashKey)) {
            if (globalState.usesTSAF()) isRandomlyGenerateTimestamp.put(hashKey, false);
            else isRandomlyGenerateTimestamp.put(hashKey, Randomly.getBoolean());
            lastTimestamp.put(hashKey, globalState.getOptions().getStartTimestampOfTSData());
        }
    }

    public static SQLQueryAdapter insertRow(TDengineGlobalState globalState) throws SQLException {
        TDengineTable table = globalState.getSchema().getRandomTable();
        return insertRow(globalState, table);
    }

    public static SQLQueryAdapter insertRow(TDengineGlobalState globalState, TDengineTable table) throws SQLException {
        if (Randomly.getBoolean()) {
            return new TDengineInsertGenerator(globalState, table).generateInsert();
        } else {
            return new TDengineInsertGenerator(globalState, table).generateInsert();
        }
    }

    private SQLQueryAdapter generateReplace() {
        sb.append("REPLACE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED"));
        }
//        return generateInto();
        return null;
    }

    private SQLQueryAdapter generateInsert() {
        sb.append("INSERT");
        sb.append(" INTO ");
        sb.append(table.getName());

        if (globalState.usesTSAF()) generateIntoForTSAF();
        else generateInto();

        TDengineErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private void generateInto() {
        // TODO using、tags
        List<TDengineColumn> columns = table.getRandomNonEmptyColumnSubsetContainsPrimaryKey();
        sb.append("(");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(") ");
        sb.append("VALUES");

        int nrRows;
        if (Randomly.getBoolean()) nrRows = 1;
        else nrRows = 1 + Randomly.smallNumber();
        TDengineExpressionGenerator gen = new TDengineExpressionGenerator(globalState);
        for (int row = 0; row < nrRows; row++) {
            if (row != 0) sb.append(", ");
            sb.append("(");
            // timestamp -> 按照采样间隔顺序插入 or 随机生成(起始时间戳 -> 至今)
            String databaseAndTableName = generateHashKey(globalState.getDatabaseName(), table.getName());
            if (isRandomlyGenerateTimestamp.get(databaseAndTableName)) {
                sb.append(globalState.getRandomTimestamp());
            } else {
                long nextTimestamp = globalState.getNextSampleTimestamp(lastTimestamp.get(databaseAndTableName));
                sb.append(nextTimestamp);
                lastTimestamp.put(databaseAndTableName, nextTimestamp);
            }
            for (int c = 1; c < columns.size(); c++) {
                sb.append(", ").append(
                        TDengineVisitor.asString(gen.generateConstantForTDengineDataType(columns.get(c).getType())));
            }
            sb.append(")");
        }
    }

    private void generateIntoForTSAF() {
        List<TDengineColumn> columns = table.getAllColumnSubsetContainsPrimaryKey();
        sb.append("(");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(") ");
        sb.append("VALUES");

        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        String databaseAndTableName = generateHashKey(databaseName, tableName);
        long startTimestamp = globalState.getNextSampleTimestamp(lastTimestamp.get(databaseAndTableName));
        long endTimestamp = startTimestamp + TDengineTableGenerator.SAMPLING_NUMBER
                * globalState.getOptions().getSamplingFrequency();
        SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection(databaseName, tableName);
        List<Long> timestamps = samplingFrequency.apply(startTimestamp, endTimestamp);
        lastTimestamp.put(databaseAndTableName, endTimestamp);
        for (long nextTimestamp : timestamps) {
            // device id
            int deviceNumber = 1;
            while (deviceNumber > 0) {
                sb.append("(");
                // columns
                boolean isNull = false;
                // 空值概率存在
                if (Randomly.getBoolean(1, 50)) {
                    TableToNullValuesManager.addNullValueToTable(databaseName, tableName, nextTimestamp);
                    isNull = true;
                }
                for (int c = 0; c < columns.size(); c++) {
                    String columnName = columns.get(c).getName();
                    TDengineSchema.TDengineDataType type = columns.get(c).getType();
                    if (columnName.equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName())) {
                        // timestamp -> 按照采样间隔顺序插入(起始时间戳 -> 至今)
                        sb.append(nextTimestamp);
                    } else if (columnName.equalsIgnoreCase(
                            TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName())) {
                        sb.append(", ").append(deviceNumber);
                    } else {
                        if (isNull) sb.append(", null");
                        else sb.append(", ").append(TDengineConstant.createBigDecimalConstant(EquationsManager
                                .getInstance()
                                .initEquationsFromTimeSeries(databaseName, tableName, columnName, type.toTSAFDataType())
                                .genValueByTimestamp(samplingFrequency, nextTimestamp)).castAs(
                                TDengineCastOperation.CastType.TDengineDataTypeToCastType(type)));
                    }
                }
                sb.append("), ");
                deviceNumber--;
            }
        }
        sb.delete(sb.length() - 2, sb.length());
    }

    public static Long getLastTimestamp(String databaseName, String tableName) {
        return lastTimestamp.get(generateHashKey(databaseName, tableName));
    }

    public static String generateHashKey(String databaseName, String tableName) {
        return String.format("%s_%s", databaseName, tableName);
    }
}
