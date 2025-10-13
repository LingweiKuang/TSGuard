package com.fuzzy.prometheus;

import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.*;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.query.SQLQueryProvider;
import com.fuzzy.prometheus.apiEntry.PrometheusQueryParam;
import com.fuzzy.prometheus.apiEntry.PrometheusRequestType;
import com.fuzzy.prometheus.gen.PrometheusDatabaseGenerator;
import com.fuzzy.prometheus.gen.PrometheusDeleteSeriesGenerator;
import com.fuzzy.prometheus.gen.PrometheusInsertGenerator;
import com.fuzzy.prometheus.gen.PrometheusTableGenerator;
import com.google.auto.service.AutoService;

import java.sql.SQLException;

@AutoService(DatabaseProvider.class)
public class PrometheusProvider extends SQLProviderAdapter<PrometheusGlobalState, PrometheusOptions> {

    public PrometheusProvider() {
        super(PrometheusGlobalState.class, PrometheusOptions.class);
    }

    enum Action implements AbstractAction<PrometheusGlobalState> {
        INSERT(PrometheusInsertGenerator::insertRow),
        DELETE_SERIES_INIT_VAL(PrometheusDeleteSeriesGenerator::generate),
        CREATE_TABLE((g) -> {
            String tableName = DBMSCommon.createTableName(g.getSchema().getMaxTableIndex() + 1);
            return PrometheusTableGenerator.generate(g, tableName);
        });

        private final SQLQueryProvider<PrometheusGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<PrometheusGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(PrometheusGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(PrometheusGlobalState globalState, Action a) {
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
    protected void recordQueryExecutionStatistical(PrometheusGlobalState globalState) {
        // record query syntax sequence
    }

    @Override
    public void generateDatabase(PrometheusGlobalState globalState) throws Exception {
        // 查询表数量，不足则继续建表
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getMaxTableIndex() + 1);
            SQLQueryAdapter createTable = PrometheusTableGenerator.generate(globalState, tableName);
            globalState.executeStatement(createTable);
        }

        // 数据生成
        StatementExecutor<PrometheusGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                PrometheusProvider::mapActions, (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(PrometheusGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) host = PrometheusOptions.DEFAULT_HOST;
        if (port == MainOptions.NO_SET_PORT) port = PrometheusOptions.DEFAULT_PORT;
        String databaseName = globalState.getDatabaseName();

        PrometheusConnection prometheusConnection = new PrometheusConnection(host, port);
        try (PrometheusStatement s = prometheusConnection.createStatement()) {
            // example: databaseName{tableName="t0", seriesName="s0"} 10 1745510400000

//            PrometheusResultSet prometheusResultSet = (PrometheusResultSet) s.executeQuery(
//                    new PrometheusQueryParam(databaseMatch)
//                            .genPrometheusRequestParam(PrometheusRequestType.SERIES_QUERY));
            // TODO Prometheus无需删除数据, 每轮测试开始时间点都不一样, 为当前时间回退 30s
//            if (prometheusResultSet.hasNext()) {
//                s.execute(JSONObject.toJSONString(new PrometheusQueryParam(
//                        PrometheusRequestType.series_delete, databaseMatch)));

//                while (pushGatewayResult.hasNext()) {
//                    JSONObject currentValue = (JSONObject) pushGatewayResult.getCurrentValue();
//                    String jobName = ((JSONObject) currentValue.get("labels")).getString("job");
//                    if (jobName.startsWith(databaseName))
//                        s.execute(JSONObject.toJSONString(new PrometheusQueryParam(
//                                PrometheusRequestType.push_gateway_series_delete, jobName)));
//                }
//            }

            // 创建数据库时插入一个初始点即可
            String queryString = PrometheusDatabaseGenerator.generate(globalState, databaseName).getQueryString();
            s.execute(queryString);
        }

        if (prometheusConnection.isClosed()) {
            throw new SQLException("createDatabase error...");
        }
        return new SQLConnection(prometheusConnection);
    }

    @Override
    public void dropDatabase(PrometheusGlobalState globalState) throws Exception {
        // POST /api/v1/admin/tsdb/delete_series
        String databaseMatch = String.format("match[]={__name__=\"%s\"}", globalState.getDatabaseName());
        // 删除时间序列
        try (TSFuzzyStatement s = globalState.getConnection().createStatement()) {
            String deleteRequest = new PrometheusQueryParam(databaseMatch)
                    .genPrometheusRequestParam(PrometheusRequestType.SERIES_DELETE);
            s.execute(deleteRequest);
        }
    }

    @Override
    public String getDBMSName() {
        return GlobalConstant.PROMETHEUS_DATABASE_NAME;
    }

    @Override
    public boolean addRowsToAllTables(PrometheusGlobalState globalState) throws Exception {
        return true;
    }

}
