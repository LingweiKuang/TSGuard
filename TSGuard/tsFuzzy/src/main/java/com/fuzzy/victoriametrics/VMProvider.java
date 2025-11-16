package com.fuzzy.victoriametrics;

import com.alibaba.fastjson.JSONObject;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.*;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.query.SQLQueryProvider;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.victoriametrics.apientry.VMRequestParam;
import com.fuzzy.victoriametrics.apientry.VMRequestType;
import com.fuzzy.victoriametrics.gen.VMDatabaseGenerator;
import com.fuzzy.victoriametrics.gen.VMDeleteSeriesGenerator;
import com.fuzzy.victoriametrics.gen.VMInsertGenerator;
import com.fuzzy.victoriametrics.gen.VMTableGenerator;
import com.google.auto.service.AutoService;

import java.sql.SQLException;

@AutoService(DatabaseProvider.class)
public class VMProvider extends SQLProviderAdapter<VMGlobalState, VMOptions> {

    public VMProvider() {
        super(VMGlobalState.class, VMOptions.class);
    }

    enum Action implements AbstractAction<VMGlobalState> {
        INSERT(VMInsertGenerator::insertRow),
        DELETE_SERIES_INIT_VAL(VMDeleteSeriesGenerator::generate),
        CREATE_TABLE((g) -> {
            String tableName = DBMSCommon.createTableName(g.getSchema().getMaxTableIndex() + 1);
            return VMTableGenerator.generate(g, tableName);
        });

        private final SQLQueryProvider<VMGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<VMGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(VMGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(VMGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed = 0;
        switch (a) {
            case CREATE_TABLE:
                nrPerformed = r.getInteger(0, 1);
                break;
            case INSERT:
                nrPerformed = r.getInteger(3, globalState.getOptions().getMaxNumberInserts());
                break;
            case DELETE_SERIES_INIT_VAL:
                nrPerformed = r.getInteger(1, 2);
                break;
            default:
                throw new AssertionError(a);
        }
        return nrPerformed;
    }

    @Override
    protected void recordQueryExecutionStatistical(VMGlobalState globalState) {
        // record query syntax sequence
    }

    @Override
    public void generateDatabase(VMGlobalState globalState) throws Exception {
        // 查询表数量，不足则继续建表
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getMaxTableIndex() + 1);
            SQLQueryAdapter createTable = VMTableGenerator.generate(globalState, tableName);
            globalState.executeStatement(createTable);
        }

        // 数据生成
        StatementExecutor<VMGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                VMProvider::mapActions, (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(VMGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) host = VMOptions.DEFAULT_HOST;
        if (port == MainOptions.NO_SET_PORT) port = VMOptions.DEFAULT_PORT;
        String databaseName = globalState.getDatabaseName();

        VMConnection VMConnection = new VMConnection(host, port);
        try (VMStatement s = VMConnection.createStatement()) {
            // 创建数据库时插入一个初始点即可
            String queryString = VMDatabaseGenerator.generate(globalState, databaseName).getQueryString();
            s.execute(queryString);
        }

        if (VMConnection.isClosed()) {
            throw new SQLException("createDatabase error...");
        }
        return new SQLConnection(VMConnection);
    }

    @Override
    public void dropDatabase(VMGlobalState globalState) throws Exception {
        // 删除时间序列
        try (TSFuzzyStatement s = globalState.getConnection().createStatement()) {
            JSONObject requestBody = new JSONObject();
            // match[] = {__name__="metricName_value"}
            requestBody.put("match[]", String.format("{__name__=\"%s\"}",
                    globalState.getDatabaseName() + TimeSeriesLabelConstant.END_WITH_VALUE.getLabel()));
            s.execute(JSONObject.toJSONString(new VMRequestParam(VMRequestType.SERIES_DELETE, requestBody.toJSONString())));
        }
    }

    @Override
    public String getDBMSName() {
        return GlobalConstant.VICTORIA_METRICS_DATABASE_NAME;
    }

    @Override
    public boolean addRowsToAllTables(VMGlobalState globalState) throws Exception {
        return true;
    }

}
