package com.fuzzy.victoriametrics.oracle;


import com.alibaba.fastjson.JSONObject;
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
import com.fuzzy.victoriametrics.VMErrors;
import com.fuzzy.victoriametrics.VMGlobalState;
import com.fuzzy.victoriametrics.VMSchema;
import com.fuzzy.victoriametrics.VMSchema.VMColumn;
import com.fuzzy.victoriametrics.VMSchema.VMTable;
import com.fuzzy.victoriametrics.VMVisitor;
import com.fuzzy.victoriametrics.apientry.VMRequestParam;
import com.fuzzy.victoriametrics.apientry.VMRequestType;
import com.fuzzy.victoriametrics.apientry.entity.VMRangeQueryRequest;
import com.fuzzy.victoriametrics.ast.VMColumnReference;
import com.fuzzy.victoriametrics.ast.VMExpression;
import com.fuzzy.victoriametrics.ast.VMSelect;
import com.fuzzy.victoriametrics.ast.VMTableReference;
import com.fuzzy.victoriametrics.feedback.VMQuerySynthesisFeedbackManager;
import com.fuzzy.victoriametrics.gen.VMExpressionGenerator;
import com.fuzzy.victoriametrics.resultset.VMResultSet;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class VMStreamComputingOracle
        extends TimeSeriesStreamComputingBase<VMGlobalState, VMExpression, SQLConnection> {

    private List<VMExpression> fetchColumns;
    private List<VMColumn> columns;
    private VMTable table;
    private VMExpression whereClause;
    VMSelect selectStatement;

    public VMStreamComputingOracle(VMGlobalState globalState) {
        super(globalState);
        VMErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getTimeSeriesQuery() {
        VMSchema schema = globalState.getSchema();
        VMSchema.VMTables randomFromTables = schema.getRandomTableNonEmptyTables();
        List<VMTable> tables = randomFromTables.getTables();
        selectStatement = new VMSelect();
        table = Randomly.fromList(tables);
        columns = randomFromTables.getColumns();
        selectStatement.setFromList(tables.stream().map(VMTableReference::new).collect(Collectors.toList()));

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
//            selectStatement.setTimeSeriesFunction(VMTimeSeriesFunc.getRandomFunction(
//                    columns, globalState.getRandomly()));
//        }

        // fetchColumns
        fetchColumns = columns.stream().map(c -> new VMColumnReference(c, null))
                .collect(Collectors.toList());
        selectStatement.setFetchColumns(fetchColumns);

        // 时间范围全选
        String metricQL = VMVisitor.asString(selectStatement);
        // EndTime 随机选，预期结果集跟随 endtime
        VMRangeQueryRequest request = new VMRangeQueryRequest(metricQL, "1s",
                globalState.getOptions().getStartTimestampOfTSData(),
                globalState.getOptions().getStartTimestampOfTSData() + 100 * 1000);
        // INSTANT_QUERY or RANGE_QUERY
        return new SQLQueryAdapter(JSONObject.toJSONString(new VMRequestParam(VMRequestType.RANGE_QUERY,
                JSONObject.toJSONString(request))), errors);
    }

    private VMExpression generateExpression(List<VMColumn> columns) {
        VMExpression predicateExpression = null;
        boolean reGenerateExpr;
        int regenerateCounter = 0;
        String predicateSequence = "";
        do {
            reGenerateExpr = false;
            try {
                predicateExpression = new VMExpressionGenerator(globalState).setColumns(columns).generateExpression();

                // 结果解析
                predicateSequence = VMVisitor.asString(predicateExpression, true);
                if (predicateExpression.isScalarExpression()) {
                    // 仅含标量的表达式不进行度量
                    throw new ReGenerateExpressionException(String.format("该语法节点序列仅含标量, 需重新生成:%s",
                            predicateSequence));
                }

                if (globalState.getOptions().useSyntaxSequence()
                        && VMQuerySynthesisFeedbackManager.isRegenerateSequence(predicateSequence)) {
                    regenerateCounter++;
                    if (regenerateCounter >= VMQuerySynthesisFeedbackManager.MAX_REGENERATE_COUNT_PER_ROUND) {
                        VMQuerySynthesisFeedbackManager.incrementExpressionDepth(
                                globalState.getOptions().getMaxExpressionDepth());
                        regenerateCounter = 0;
                    }
                    throw new ReGenerateExpressionException(String.format("该语法节点序列需重新生成:%s", predicateSequence));
                }
                // 更新概率表
                VMQuerySynthesisFeedbackManager.addSequenceRegenerateProbability(predicateSequence);
            } catch (ReGenerateExpressionException e) {
                reGenerateExpr = true;
            }
        } while (reGenerateExpr);

        this.predicate = predicateExpression;
        this.predicateSequence = predicateSequence;
        return predicateExpression;
    }

    @Override
    protected void setSequenceRegenerateProbabilityToMax(String sequence) {
        VMQuerySynthesisFeedbackManager.setSequenceRegenerateProbabilityToMax(sequence);
    }

    @Override
    protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType) {
        switch (queryType) {
            case error:
                VMQuerySynthesisFeedbackManager.incrementErrorQueryCount();
                break;
            case invalid:
                VMQuerySynthesisFeedbackManager.incrementInvalidQueryCount();
                break;
            case success:
                VMQuerySynthesisFeedbackManager.incrementSuccessQueryCount();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Invalid QueryExecutionType: %s", queryType));
        }
    }

    @Override
    protected TimeSeriesStream getExpectedValues(VMExpression expression, long endTimestamp) {
        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        List<String> fetchColumnNames = columns.stream().map(AbstractTableColumn::getName).collect(Collectors.toList());
        // 表达式计算
        return VMVisitor.streamComputeTimeSeriesVector(databaseName,
                fetchColumnNames, globalState.getOptions().getStartTimestampOfTSData(), endTimestamp, expression,
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
//        VMResultSet VMResultSet = (VMResultSet) result;
//        Map<Long, List<BigDecimal>> resultSet = null;
//        if (!result.hasNext()) {
//            if (selectStatement.getQueryType().isTimeWindowQuery()) return expectedResultSet.isEmpty();
//            else
//                return VMTimeSeriesFunc.apply(selectStatement.getTimeSeriesFunction(), expectedResultSet).isEmpty();
//        }
//
//        // 窗口聚合 -> 预期结果集
//        if (selectStatement.getQueryType().isTimeWindowQuery()) {
//            VMColumn timeColumn = new VMColumn(VMValueStateConstant.TIME_FIELD.getValue(),
//                    false, VMDataType.TIMESTAMP);
//            int timeIndex = VMResultSet.findColumn(timeColumn.getName());
//            // 获取窗口划分初始时间戳
//            long startTimestamp = getConstantFromResultSet(VMResultSet.getCurrentValue().getValues().get(0),
//                    timeColumn.getType(), timeIndex).getBigDecimalValue().longValue();
//            String intervalVal = selectStatement.getIntervalValues().get(0);
//            long duration = globalState.transTimestampToPrecision(
//                    Long.parseLong(intervalVal.substring(0, intervalVal.length() - 1)) * 1000);
//            resultSet = selectStatement.getAggregationType()
//                    .apply(expectedResultSet, startTimestamp, duration, duration);
//        } else if (selectStatement.getQueryType().isTimeSeriesFunction())
//            resultSet = VMTimeSeriesFunc.apply(selectStatement.getTimeSeriesFunction(), expectedResultSet);
//
//        // 验证结果
//        result.resetCursor();
//        boolean verifyRes = verifyGeneralQuery(resultSet, result);
//        if (!verifyRes) logAggregationSet(resultSet);
//        return verifyRes;
        return true;
    }

    /**
     * VM 按照 step 取数，对于不存在的点自动往前取最近点数据。
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
        VMResultSet VMResultSet = (VMResultSet) result;
        // 解析 DBValResultSet
        try {
            TimeSeriesStream actualResultSet = VMResultSet.genTimeSeriesStream();
            return actualResultSet.containsAndEquals(expectedResultSet) ? VerifyResultState.SUCCESS : VerifyResultState.FAIL;
        } catch (Exception e) {
            log.error("解码 JSON 格式异常, json:{} e:", VMResultSet.getJsonResult(), e);
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

    //    private VMConstant getConstantFromResultSet(List<String> resultSet, VMDataType dataType,
//                                                      int columnIndex) {
//        if (resultSet.get(columnIndex).equalsIgnoreCase("null")) return VMConstant.createNullConstant();
//
//        VMConstant constant;
//        switch (dataType) {
//            case BOOLEAN:
//                constant = VMConstant.createBoolean(
//                        resultSet.get(columnIndex).equalsIgnoreCase("true"));
//                break;
//            case STRING:
//                constant = VMConstant.createSingleQuotesStringConstant(resultSet.get(columnIndex));
//                break;
//            case INT:
//            case UINT:
//            case FLOAT:
//            case BIGDECIMAL:
//                constant = VMConstant.createBigDecimalConstant(
//                        new BigDecimal(resultSet.get(columnIndex)));
//                break;
//            case TIMESTAMP:
//                // 时间格式 -> 直接返回, 不再进行空值判定
//                constant = VMConstant.createBigDecimalConstant(
//                        new BigDecimal(globalState.transDateToTimestamp(resultSet.get(columnIndex))));
//                break;
//            default:
//                throw new AssertionError(dataType);
//        }
//        if (selectStatement.getAggregationType() != null
//                && VMAggregationType.getVMAggregationType(selectStatement.getAggregationType()).isFillZero()
//                && constant.getBigDecimalValue().compareTo(BigDecimal.ZERO) == 0
//                && dataType != VMDataType.TIMESTAMP)
//            return VMConstant.createNullConstant();
//        return constant;
//    }

//    private void generateTimeWindowClause() {
//        selectStatement.setAggregationType(VMAggregationType.getRandomAggregationType());
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

//    private List<VMExpression> generateGroupByClause(List<VMColumn> columns, VMRowValue rw) {
//        if (Randomly.getBoolean()) {
//            return columns.stream().map(c -> VMColumnReference.create(c, rw.getValues().get(c)))
//                    .collect(Collectors.toList());
//        } else {
//            return Collections.emptyList();
//        }
//    }
}
