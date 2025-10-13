package com.fuzzy.prometheus.oracle;


import com.benchmark.entity.DBValResultSet;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.oracle.TimeSeriesStreamComputingBase;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.schema.AbstractTableColumn;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.common.tsaf.QueryType;
import com.fuzzy.common.tsaf.TableToNullValuesManager;
import com.fuzzy.prometheus.PrometheusErrors;
import com.fuzzy.prometheus.PrometheusGlobalState;
import com.fuzzy.prometheus.PrometheusSchema;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusColumn;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusTable;
import com.fuzzy.prometheus.PrometheusVisitor;
import com.fuzzy.prometheus.apiEntry.PrometheusQueryParam;
import com.fuzzy.prometheus.apiEntry.PrometheusRequestType;
import com.fuzzy.prometheus.ast.PrometheusColumnReference;
import com.fuzzy.prometheus.ast.PrometheusExpression;
import com.fuzzy.prometheus.ast.PrometheusSelect;
import com.fuzzy.prometheus.ast.PrometheusTableReference;
import com.fuzzy.prometheus.feedback.PrometheusQuerySynthesisFeedbackManager;
import com.fuzzy.prometheus.gen.PrometheusExpressionGenerator;
import com.fuzzy.prometheus.resultSet.PrometheusResultSet;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class PrometheusStreamComputingOracle
        extends TimeSeriesStreamComputingBase<PrometheusGlobalState, PrometheusExpression, SQLConnection> {

    private List<PrometheusExpression> fetchColumns;
    private List<PrometheusColumn> columns;
    private PrometheusTable table;
    private PrometheusExpression whereClause;
    PrometheusSelect selectStatement;

    public PrometheusStreamComputingOracle(PrometheusGlobalState globalState) {
        super(globalState);
        PrometheusErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getTimeSeriesQuery() {
        PrometheusSchema schema = globalState.getSchema();
        PrometheusSchema.PrometheusTables randomFromTables = schema.getRandomTableNonEmptyTables();
        List<PrometheusTable> tables = randomFromTables.getTables();
        selectStatement = new PrometheusSelect();
        table = Randomly.fromList(tables);
        columns = randomFromTables.getColumns();
        selectStatement.setFromList(tables.stream().map(PrometheusTableReference::new).collect(Collectors.toList()));

        // TODO
//        QueryType queryType = Randomly.fromOptions(QueryType.values());
        QueryType queryType = Randomly.fromOptions(QueryType.BASE_QUERY);
        whereClause = generateExpression(columns);
        selectStatement.setWhereClause(whereClause);
        selectStatement.setQueryType(queryType);

        // TODO
//        if (queryType.isTimeWindowQuery()) {
//            // 随机窗口查询测试
//            generateTimeWindowClause(columns);
//        }
//        // TimeSeries Function
//        else if (queryType.isTimeSeriesFunction()) {
//            // 随机窗口查询测试
//            selectStatement.setTimeSeriesFunction(PrometheusTimeSeriesFunc.getRandomFunction(
//                    columns, globalState.getRandomly()));
//        }

        // fetchColumns
        fetchColumns = columns.stream().map(c -> new PrometheusColumnReference(c, null))
                .collect(Collectors.toList());
//        String timeColumnName = queryType.isTimeWindowQuery() ? PrometheusConstantString.W_START_TIME_COLUMN_NAME.getName() :
//                PrometheusConstantString.TIME_FIELD_NAME.getName();
//        fetchColumns.add(new PrometheusColumnReference(new PrometheusSchema.PrometheusColumn(timeColumnName, false,
//                PrometheusSchema.PrometheusDataType.TIMESTAMP), null));
        selectStatement.setFetchColumns(fetchColumns);

        // TODO 时间范围全选 => 筛选范围
        long queryStart = globalState.getOptions().getStartTimestampOfTSData() - 500 * 1000;
        return new SQLQueryAdapter(new PrometheusQueryParam(PrometheusVisitor.asString(selectStatement), queryStart,
                System.currentTimeMillis()).genPrometheusRequestParam(PrometheusRequestType.RANGE_QUERY), errors);
    }

    private PrometheusExpression generateExpression(List<PrometheusColumn> columns) {
        PrometheusExpression result = null;
        boolean reGenerateExpr;
        int regenerateCounter = 0;
        String predicateSequence = "";
        do {
            reGenerateExpr = false;
            try {
                PrometheusExpression predicateExpression =
                        new PrometheusExpressionGenerator(globalState).setColumns(columns).generateExpression();

                // add time column
//                PrometheusExpression timeExpression = new PrometheusTimeExpressionGenerator(globalState).setColumns(
//                        Collections.singletonList(new PrometheusSchema.PrometheusColumn(PrometheusConstantString.TIME_FIELD_NAME.getName(),
//                                false, PrometheusSchema.PrometheusDataType.TIMESTAMP))).generateExpression();
//                PrometheusExpression timeExpression = null;

//                result = new PrometheusBinaryLogicalOperation(rectifiedPredicateExpression, timeExpression,
//                        PrometheusBinaryLogicalOperation.PrometheusBinaryLogicalOperator.AND);
                // TODO 暂时忽略时间维度
                result = predicateExpression;

                // TODO 结果解析
                predicateSequence = PrometheusVisitor.asString(predicateExpression, true);
                if (result.isScalarExpression()) {
                    // 仅含标量的表达式不进行度量
                    throw new ReGenerateExpressionException(String.format("该语法节点序列仅含标量, 需重新生成:%s",
                            predicateSequence));
                }

                if (globalState.getOptions().useSyntaxSequence()
                        && PrometheusQuerySynthesisFeedbackManager.isRegenerateSequence(predicateSequence)) {
                    regenerateCounter++;
                    if (regenerateCounter >= PrometheusQuerySynthesisFeedbackManager.MAX_REGENERATE_COUNT_PER_ROUND) {
                        PrometheusQuerySynthesisFeedbackManager.incrementExpressionDepth(
                                globalState.getOptions().getMaxExpressionDepth());
                        regenerateCounter = 0;
                    }
                    throw new ReGenerateExpressionException(String.format("该语法节点序列需重新生成:%s", predicateSequence));
                }
                // 更新概率表
                PrometheusQuerySynthesisFeedbackManager.addSequenceRegenerateProbability(predicateSequence);
            } catch (ReGenerateExpressionException e) {
                reGenerateExpr = true;
            }
        } while (reGenerateExpr);

        log.info("Expression: {}", PrometheusVisitor.asString(result));
        this.predicate = result;
        this.predicateSequence = predicateSequence;
        return result;
    }

    @Override
    protected void setSequenceRegenerateProbabilityToMax(String sequence) {
        PrometheusQuerySynthesisFeedbackManager.setSequenceRegenerateProbabilityToMax(sequence);
    }

    @Override
    protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType) {
        switch (queryType) {
            case error:
                PrometheusQuerySynthesisFeedbackManager.incrementErrorQueryCount();
                break;
            case invalid:
                PrometheusQuerySynthesisFeedbackManager.incrementInvalidQueryCount();
                break;
            case success:
                PrometheusQuerySynthesisFeedbackManager.incrementSuccessQueryCount();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Invalid QueryExecutionType: %s", queryType));
        }
    }

    @Override
    protected TimeSeriesStream getExpectedValues(PrometheusExpression expression) {
        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        List<String> fetchColumnNames = columns.stream().map(AbstractTableColumn::getName).collect(Collectors.toList());
        // 表达式计算
        return PrometheusVisitor.streamComputeTimeSeriesVector(databaseName,
                fetchColumnNames, globalState.getOptions().getStartTimestampOfTSData(), expression,
                TableToNullValuesManager.getNullValues(databaseName, tableName));
    }

    @Override
    protected boolean verifyResultSet(TimeSeriesStream expectedResultSet, DBValResultSet result) {
        try {
            if (selectStatement.getQueryType().isTimeSeriesFunction()
                    || selectStatement.getQueryType().isTimeWindowQuery())
                return verifyTimeWindowQuery(expectedResultSet, result);
            else return verifyGeneralQuery(expectedResultSet, result);
        } catch (Exception e) {
            log.error("验证查询结果集和预期结果集等价性异常, e:", e);
            return false;
        }
    }

    private boolean verifyTimeWindowQuery(TimeSeriesStream expectedResultSet, DBValResultSet result)
            throws Exception {
//        // 对expectedResultSet进行聚合, 聚合结果按照时间戳作为Key, 进行进一步数值比较
//        PrometheusResultSet PrometheusResultSet = (PrometheusResultSet) result;
//        Map<Long, List<BigDecimal>> resultSet = null;
//        if (!result.hasNext()) {
//            if (selectStatement.getQueryType().isTimeWindowQuery()) return expectedResultSet.isEmpty();
//            else
//                return PrometheusTimeSeriesFunc.apply(selectStatement.getTimeSeriesFunction(), expectedResultSet).isEmpty();
//        }
//
//        // 窗口聚合 -> 预期结果集
//        if (selectStatement.getQueryType().isTimeWindowQuery()) {
//            PrometheusColumn timeColumn = new PrometheusColumn(PrometheusValueStateConstant.TIME_FIELD.getValue(),
//                    false, PrometheusDataType.TIMESTAMP);
//            int timeIndex = PrometheusResultSet.findColumn(timeColumn.getName());
//            // 获取窗口划分初始时间戳
//            long startTimestamp = getConstantFromResultSet(PrometheusResultSet.getCurrentValue().getValues().get(0),
//                    timeColumn.getType(), timeIndex).getBigDecimalValue().longValue();
//            String intervalVal = selectStatement.getIntervalValues().get(0);
//            long duration = globalState.transTimestampToPrecision(
//                    Long.parseLong(intervalVal.substring(0, intervalVal.length() - 1)) * 1000);
//            resultSet = selectStatement.getAggregationType()
//                    .apply(expectedResultSet, startTimestamp, duration, duration);
//        } else if (selectStatement.getQueryType().isTimeSeriesFunction())
//            resultSet = PrometheusTimeSeriesFunc.apply(selectStatement.getTimeSeriesFunction(), expectedResultSet);
//
//        // 验证结果
//        result.resetCursor();
//        boolean verifyRes = verifyGeneralQuery(resultSet, result);
//        if (!verifyRes) logAggregationSet(resultSet);
//        return verifyRes;
        return true;
    }

    /**
     * Prometheus 按照 step 取数，对于不存在的点自动往前取最近点数据。
     * 因此，比对逻辑：真实结果集一定包含预期结果集所有数据。
     *
     * @param expectedResultSet
     * @param result
     * @return
     * @throws Exception
     */
    private boolean verifyGeneralQuery(TimeSeriesStream expectedResultSet, DBValResultSet result)
            throws Exception {
        // 验证包含关系
        return verifyRowResult(expectedResultSet, result) != VerifyResultState.FAIL;
    }

    private VerifyResultState verifyRowResult(TimeSeriesStream expectedResultSet, DBValResultSet result)
            throws Exception {
        PrometheusResultSet prometheusResultSet = (PrometheusResultSet) result;
        // 解析 DBValResultSet
        try {
            TimeSeriesStream actualResultSet = prometheusResultSet.genTimeSeriesStream();
            return actualResultSet.containsAndEquals(expectedResultSet) ? VerifyResultState.SUCCESS : VerifyResultState.FAIL;
        } catch (Exception e) {
            log.error("解码 JSON 格式异常, json:{} e:", prometheusResultSet.getJsonResult(), e);
            return VerifyResultState.FAIL;
        }
    }

    private VerifyResultState compareSortedValues(List<BigDecimal> expectedValues, List<BigDecimal> actualValues) {
        expectedValues.sort(new Comparator<BigDecimal>() {
            @Override
            public int compare(BigDecimal o1, BigDecimal o2) {
                return o1.compareTo(o2);
            }
        });
        actualValues.sort(new Comparator<BigDecimal>() {
            @Override
            public int compare(BigDecimal o1, BigDecimal o2) {
                return o1.compareTo(o2);
            }
        });
        // 比较排序后的结果值
        for (int i = 0; i < expectedValues.size(); i++) {
            if (expectedValues.get(i).compareTo(actualValues.get(i)) != 0) {
                return VerifyResultState.FAIL;
            }
        }
        return VerifyResultState.SUCCESS;
    }

    private enum VerifyResultState {
        SUCCESS, FAIL, IS_NULL
    }

    private void logAggregationSet(Map<Long, List<BigDecimal>> aggregationResultSet) {
        globalState.getState().getLocalState().log(String.format("aggregationResultSet size:%d %s",
                aggregationResultSet.size(), aggregationResultSet));
    }

    //    private PrometheusConstant getConstantFromResultSet(List<String> resultSet, PrometheusDataType dataType,
//                                                      int columnIndex) {
//        if (resultSet.get(columnIndex).equalsIgnoreCase("null")) return PrometheusConstant.createNullConstant();
//
//        PrometheusConstant constant;
//        switch (dataType) {
//            case BOOLEAN:
//                constant = PrometheusConstant.createBoolean(
//                        resultSet.get(columnIndex).equalsIgnoreCase("true"));
//                break;
//            case STRING:
//                constant = PrometheusConstant.createSingleQuotesStringConstant(resultSet.get(columnIndex));
//                break;
//            case INT:
//            case UINT:
//            case FLOAT:
//            case BIGDECIMAL:
//                constant = PrometheusConstant.createBigDecimalConstant(
//                        new BigDecimal(resultSet.get(columnIndex)));
//                break;
//            case TIMESTAMP:
//                // 时间格式 -> 直接返回, 不再进行空值判定
//                constant = PrometheusConstant.createBigDecimalConstant(
//                        new BigDecimal(globalState.transDateToTimestamp(resultSet.get(columnIndex))));
//                break;
//            default:
//                throw new AssertionError(dataType);
//        }
//        if (selectStatement.getAggregationType() != null
//                && PrometheusAggregationType.getPrometheusAggregationType(selectStatement.getAggregationType()).isFillZero()
//                && constant.getBigDecimalValue().compareTo(BigDecimal.ZERO) == 0
//                && dataType != PrometheusDataType.TIMESTAMP)
//            return PrometheusConstant.createNullConstant();
//        return constant;
//    }

//    private void generateTimeWindowClause() {
//        selectStatement.setAggregationType(PrometheusAggregationType.getRandomAggregationType());
//
//        List<String> intervals = new ArrayList<>();
//        // TODO 采样点前后1000s 取决于采样点endStartTimestamp
//        // TODO 时间范围
//        long timestampInterval = 1000 * 1000000L * 1000;
//        String timeUnit = "s";
//        long windowSize = globalState.getRandomly().getLong(100000, timestampInterval / 1000);
//        // TODO OFFSETSIZE 数值应用后返回大量无效值
////        long offsetSize = globalState.getRandomly().getLong(100000, timestampInterval / 1000);
//        intervals.add(String.format(String.format("%d%s", windowSize, timeUnit)));
////        intervals.add(String.format(String.format("%d%s", offsetSize, timeUnit)));
//        selectStatement.setIntervalValues(intervals);
//    }

//    private List<PrometheusExpression> generateGroupByClause(List<PrometheusColumn> columns, PrometheusRowValue rw) {
//        if (Randomly.getBoolean()) {
//            return columns.stream().map(c -> PrometheusColumnReference.create(c, rw.getValues().get(c)))
//                    .collect(Collectors.toList());
//        } else {
//            return Collections.emptyList();
//        }
//    }
}
