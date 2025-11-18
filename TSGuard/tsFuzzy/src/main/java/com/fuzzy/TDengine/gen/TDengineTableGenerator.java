package com.fuzzy.TDengine.gen;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.TDengine.TDengineGlobalState;
import com.fuzzy.TDengine.TDengineSchema.TDengineDataType;
import com.fuzzy.TDengine.tsaf.enu.TDengineConstantString;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TDengineTableGenerator {
    public static final long SAMPLING_NUMBER = 30;
    private final StringBuilder sb = new StringBuilder();
    private final boolean allowPrimaryKey;
    private boolean setPrimaryKey;
    private final String tableName;
    private final String superTableName;
    private final List<String> columns = new ArrayList<>();
    private final TDengineGlobalState globalState;

    public TDengineTableGenerator(TDengineGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.superTableName = "super_" + tableName;
        allowPrimaryKey = Randomly.getBoolean();
        this.globalState = globalState;
        SamplingFrequencyManager.getInstance().addSamplingFrequency(globalState.getDatabaseName(), tableName,
                globalState.getOptions().getStartTimestampOfTSData(),
                SAMPLING_NUMBER * globalState.getOptions().getSamplingFrequency(), SAMPLING_NUMBER);
    }

    public static SQLQueryAdapter generate(TDengineGlobalState globalState, String tableName) {
        return new TDengineTableGenerator(globalState, tableName).create();
    }

    private SQLQueryAdapter create() {
        if (Randomly.getBoolean()) {
            createSuperTable();
            if (Randomly.getBoolean()) createIndex();
            createSubTable();
        } else createOrdinaryTable();
        return new SQLQueryAdapter(sb.toString(), new ExpectedErrors(), true);
    }

    private void createIndex() {
        SQLQueryAdapter indexQueryAdapter = TDengineIndexGenerator.generate(globalState, superTableName);
        try {
            globalState.executeStatement(indexQueryAdapter);
        } catch (Exception e) {
            // 忽略重复创建
        }
    }

    public void createSuperTable() {
        // super table
        sb.append("CREATE STABLE").append(" IF NOT EXISTS ").append(superTableName);
        sb.append(String.format("( %s TIMESTAMP, ", TDengineConstantString.TIME_FIELD_NAME.getName()));
        if (globalState.usesTSAF())
            sb.append(String.format("%s INT, ", TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName()));
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            if (i != 0) sb.append(", ");
            appendColumn(i);
        }
        sb.append(")").append(" TAGS").append(" (");
        sb.append("location BINARY(64), ").append("group_id INT").append(");");
        try {
            globalState.executeStatement(new SQLQueryAdapter(sb.toString(), new ExpectedErrors(), false));
        } catch (Exception e) {
            log.error("创建超集表失败!");
            throw new IgnoreMeException();
        }
    }

    public void createSubTable() {
        // sub table
        sb.delete(0, sb.length());
        String locationTag = globalState.getRandomly().getString()
                .replace("\\", "").replace("\n", "");
        int groupId = globalState.getRandomly().getInteger();
        sb.append("CREATE TABLE ").append(tableName)
                .append(" USING ").append(superTableName)
                .append(" TAGS ('").append(locationTag).append("', ").append(groupId).append(") ");
        appendTableOptions();
    }

    public void createOrdinaryTable() {
        sb.append("CREATE");
        sb.append(" TABLE");
        if (Randomly.getBoolean()) sb.append(" IF NOT EXISTS");
        sb.append(" ");
        sb.append(tableName);
        sb.append(String.format("( %s TIMESTAMP, ", TDengineConstantString.TIME_FIELD_NAME.getName()));
        if (globalState.usesTSAF())
            sb.append(String.format("%s INT, ", TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName()));
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            if (i != 0) sb.append(", ");
            appendColumn(i);
        }
        sb.append(")");
        sb.append(" ");
        appendTableOptions();
    }

    private void appendColumn(int columnId) {
        String columnName = DBMSCommon.createColumnName(columnId);
        columns.add(columnName);
        sb.append(columnName);
        appendColumnDefinition();
    }

    private void appendColumnDefinition() {
        sb.append(" ");
        TDengineDataType randomType = TDengineDataType.getRandom(globalState);
        sb.append(randomType.getTypeName());

        // 字符串必须附带长度, 非字符串不能附带长度
        if (TDengineDataType.BINARY == randomType || TDengineDataType.VARCHAR == randomType) {
            sb.append("(");
            sb.append(Randomly.getNotCachedInteger(30, 255));
            sb.append(")");
        }
        sb.append(" ");
        // TODO
//        appendColumnOption(randomType);
    }

    private enum ColumnOptions {
        COMMENT, PRIMARY_KEY, // TODO ENCODE, COMPRESS, LEVEL
    }

    private void appendColumnOption(TDengineDataType type) {
        Randomly randomly = globalState.getRandomly();
        boolean isNull = false;
        List<ColumnOptions> columnOptions = Randomly.subset(ColumnOptions.values());
        if (!type.isIntOrString()) {
            columnOptions.remove(ColumnOptions.PRIMARY_KEY);
        }
        for (ColumnOptions o : columnOptions) {
            sb.append(" ");
            switch (o) {
                case COMMENT:
                    sb.append(String.format("COMMENT '%s'",
                            randomly.getString().replace("\\", "").replace("\n", "")));
                    break;
                case PRIMARY_KEY:
                    // PRIMARY KEYs cannot be NULL
                    if (allowPrimaryKey && !setPrimaryKey && !isNull) {
                        sb.append("PRIMARY KEY");
                        setPrimaryKey = true;
                    }
                    break;
//                case ENCODE:
//                case COMPRESS:
//                case LEVEL:
                default:
                    throw new AssertionError();
            }
        }
    }

    // Table Option
    private enum TableOptions {
        COMMENT, TTL;
    }

    private void appendTableOptions() {
        Randomly randomly = globalState.getRandomly();
        List<TableOptions> tableOptions = Randomly.subset(TableOptions.values());
        int i = 0;
        for (TableOptions o : tableOptions) {
            switch (o) {
                case COMMENT:
                    sb.append(String.format("COMMENT '%s' ",
                            randomly.getString().replace("\\", "").replace("\n", "")));
                    break;
                case TTL:
                    sb.append(String.format("TTL %d ", Randomly.smallNumber()));
                    break;
                default:
                    throw new AssertionError(o);
            }
        }
    }
}
