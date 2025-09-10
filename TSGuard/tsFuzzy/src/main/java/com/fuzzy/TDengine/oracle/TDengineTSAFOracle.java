package com.fuzzy.TDengine.oracle;


import com.benchmark.entity.DBValResultSet;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.TDengine.TDengineErrors;
import com.fuzzy.TDengine.TDengineGlobalState;
import com.fuzzy.TDengine.TDengineSchema;
import com.fuzzy.TDengine.TDengineSchema.TDengineColumn;
import com.fuzzy.TDengine.TDengineSchema.TDengineRowValue;
import com.fuzzy.TDengine.TDengineSchema.TDengineTable;
import com.fuzzy.TDengine.TDengineSchema.TDengineTables;
import com.fuzzy.TDengine.TDengineVisitor;
import com.fuzzy.TDengine.ast.*;
import com.fuzzy.TDengine.feedback.TDengineQuerySynthesisFeedbackManager;
import com.fuzzy.TDengine.gen.TDengineExpressionGenerator;
import com.fuzzy.TDengine.gen.TDengineInsertGenerator;
import com.fuzzy.TDengine.gen.TDengineTimeExpressionGenerator;
import com.fuzzy.TDengine.tsaf.enu.TDengineAggregationType;
import com.fuzzy.TDengine.tsaf.enu.TDengineConstantString;
import com.fuzzy.TDengine.tsaf.enu.TDengineTimeSeriesFunc;
import com.fuzzy.TDengine.tsaf.template.TDengineTimeSeriesTemplate;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.oracle.TimeSeriesAlgebraFrameworkBase;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.schema.AbstractTableColumn;
import com.fuzzy.common.tsaf.PredicationEquation;
import com.fuzzy.common.tsaf.QueryType;
import com.fuzzy.common.tsaf.TableToNullValuesManager;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.common.util.TimeCost;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class TDengineTSAFOracle
        extends TimeSeriesAlgebraFrameworkBase<TDengineGlobalState, TDengineExpression, SQLConnection> {

    // TODO 插入数据空间概率分布
    // 数值和时间戳具备二元函数关系（能否知微见著，反映出各种不规律数值空间？）
    // 预期结果集由两者得出
    // TODO 完整插入所有数据后，随机更新某些时间戳数据（或者插入数据时间戳乱序，前者验证lsm树更新操作，后者验证时序数据库的合并排序）
    // TODO （时间戳和数值按列单独存储，所以两者之间的关系性不会影响到时序数据库查询？看目前是否有时序数据库压缩，查询操作考虑到多列相关性）
    private List<TDengineExpression> fetchColumns;
    private List<TDengineColumn> columns;
    private TDengineTable table;
    private TDengineExpression whereClause;
    TDengineSelect selectStatement;

    public TDengineTSAFOracle(TDengineGlobalState globalState) {
        super(globalState);
        TDengineErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getTimeSeriesQuery() {
        TDengineSchema schema = globalState.getSchema();
        TDengineTables randomFromTables = schema.getRandomTableNonEmptyTables();
        List<TDengineTable> tables = randomFromTables.getTables();
        selectStatement = new TDengineSelect();
        table = Randomly.fromList(tables);
        selectStatement.setSelectType(Randomly.fromOptions(TDengineSelect.SelectType.values()));
        // 注: TSAF和大多数时序数据库均不支持将time字段和其他字段进行比较、算术二元运算
        columns = randomFromTables.getColumns().stream()
                .filter(c -> !c.getName().equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName())
                        && !c.getName().equalsIgnoreCase(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName()))
                .collect(Collectors.toList());
        selectStatement.setFromList(tables.stream().map(TDengineTableReference::new).collect(Collectors.toList()));

//        QueryType queryType = Randomly.fromOptions(QueryType.values());
        QueryType queryType = Randomly.fromOptions(QueryType.BASE_QUERY);
        whereClause = generateExpression(columns);
        selectStatement.setWhereClause(whereClause);
        selectStatement.setQueryType(queryType);
        // 随机窗口查询测试
        if (queryType.isTimeWindowQuery()) generateTimeWindowClause(columns);
            // TimeSeries Function
        else if (queryType.isTimeSeriesFunction())
            selectStatement.setTimeSeriesFunction(TDengineTimeSeriesFunc.getRandomFunction(
                    columns, globalState.getRandomly()));
        // fetchColumns
        fetchColumns = columns.stream().map(c -> {
            String columnName = c.getName();
            if (queryType.isTimeWindowQuery())
                columnName = String.format("%s(%s)", selectStatement.getAggregationType(), columnName);
            else if (queryType.isTimeSeriesFunction())
                columnName = selectStatement.getTimeSeriesFunction().combinedArgs(columnName);
            return new TDengineColumnReference(new TDengineColumn(columnName, c.isTag(), c.getType()), null);
        }).collect(Collectors.toList());
        String timeColumnName = queryType.isTimeWindowQuery() ? TDengineConstantString.W_START_TIME_COLUMN_NAME.getName() :
                TDengineConstantString.TIME_FIELD_NAME.getName();
        fetchColumns.add(new TDengineColumnReference(new TDengineColumn(timeColumnName, false,
                TDengineSchema.TDengineDataType.TIMESTAMP), null));
        selectStatement.setFetchColumns(fetchColumns);

        // ORDER BY
        // TDengine distinct 不支持和order by time结合
        if (!(selectStatement.getQueryType().isTimeSeriesFunction()
                && selectStatement.getFromOptions() == TDengineSelect.SelectType.DISTINCT)) {
            List<TDengineExpression> orderBy = new TDengineExpressionGenerator(globalState).setColumns(
                    Collections.singletonList(new TDengineColumn(timeColumnName, false,
                            TDengineSchema.TDengineDataType.TIMESTAMP))).generateOrderBys();
            selectStatement.setOrderByExpressions(orderBy);
        }
        return new SQLQueryAdapter(TDengineVisitor.asString(selectStatement), errors);
    }

    @Override
    protected void setSequenceRegenerateProbabilityToMax(String sequence) {
        TDengineQuerySynthesisFeedbackManager.setSequenceRegenerateProbabilityToMax(sequence);
    }

    @Override
    protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType) {
        switch (queryType) {
            case error:
                TDengineQuerySynthesisFeedbackManager.incrementErrorQueryCount();
                break;
            case invalid:
                TDengineQuerySynthesisFeedbackManager.incrementInvalidQueryCount();
                break;
            case success:
                TDengineQuerySynthesisFeedbackManager.incrementSuccessQueryCount();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Invalid QueryExecutionType: %s", queryType));
        }
    }

    @Override
    protected TimeSeriesConstraint genColumnConstraint(TDengineExpression expr) {
        return TDengineVisitor.asConstraint(globalState.getDatabaseName(), table.getName(), expr,
                TableToNullValuesManager.getNullValues(globalState.getDatabaseName(), table.getName()));
    }

    @Override
    protected Map<Long, List<BigDecimal>> getExpectedValues(TDengineExpression expression) {
        // 联立方程式求解
        TimeCost timeCost = new TimeCost().begin();
        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        List<String> fetchColumnNames = columns.stream().map(AbstractTableColumn::getName).collect(Collectors.toList());
        TimeSeriesConstraint timeSeriesConstraint = TDengineVisitor.asConstraint(databaseName, tableName,
                expression, TableToNullValuesManager.getNullValues(databaseName, tableName));
        PredicationEquation predicationEquation = new PredicationEquation(timeSeriesConstraint, "equationName");
        Map<Long, List<BigDecimal>> resultSet = predicationEquation.genExpectedResultSet(databaseName, tableName, fetchColumnNames,
                globalState.getOptions().getStartTimestampOfTSData(),
                TDengineInsertGenerator.getLastTimestamp(databaseName, tableName),
                TDengineConstant.createFloatArithmeticTolerance());
        TDengineQuerySynthesisFeedbackManager.incrementCalculateTimeOverhead(timeCost.end().getCost());
        return resultSet;
    }

    @Override
    protected boolean verifyResultSet(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result) {
        try {
            if (selectStatement.getQueryType().isTimeSeriesFunction()
                    || selectStatement.getQueryType().isTimeWindowQuery())
                return verifyTimeWindowQuery(expectedResultSet, result);
            else return verifyGeneralQuery(expectedResultSet, result, 0);
        } catch (Exception e) {
            log.error("验证查询结果集和预期结果集等价性异常, e:", e);
            return false;
        }
    }

    private boolean verifyTimeWindowQuery(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result)
            throws Exception {
        // 对expectedResultSet进行聚合, 聚合结果按照时间戳作为Key, 进行进一步数值比较
        Map<Long, List<BigDecimal>> resultSet = null;
        if (!result.hasNext()) {
//            if (!expectedResultSet.isEmpty())
//                log.info("result不具备值, expectedResultSet: {}", expectedResultSet);
            if (selectStatement.getQueryType().isTimeWindowQuery()) return expectedResultSet.isEmpty();
            else
                return TDengineTimeSeriesFunc.apply(selectStatement.getTimeSeriesFunction(), expectedResultSet).isEmpty();
        }

        // 获取窗口划分初始时间戳
        int timeColumnIndex = result.findColumn(selectStatement.getQueryType().isTimeSeriesFunction() ?
                TDengineConstantString.TIME_FIELD_NAME.getName() :
                TDengineConstantString.W_START_TIME_COLUMN_NAME.getName());
        TDengineConstant timeConstant = getConstantFromResultSet(result, timeColumnIndex);
        long timestamp = timeConstant.getBigDecimalValue().longValue();

        // 窗口运算 -> 预期结果集
        if (selectStatement.getQueryType().isTimeWindowQuery()) {
            String intervalVal = selectStatement.getIntervalValues().get(0);
            String slidingValue = selectStatement.getSlidingValue();
            long duration = globalState.transTimestampToPrecision(
                    Long.parseLong(intervalVal.substring(0, intervalVal.length() - 1)) * 1000);
            long sliding = globalState.transTimestampToPrecision(
                    Long.parseLong(slidingValue.substring(0, slidingValue.length() - 1)) * 1000);
            selectStatement.getAggregationType().setSingleValueFillZero(true);
            resultSet = selectStatement.getAggregationType().apply(expectedResultSet, timestamp, duration, sliding);
        } else if (selectStatement.getQueryType().isTimeSeriesFunction()) {
            TimeCost timeCost = new TimeCost().begin();
            resultSet = TDengineTimeSeriesFunc.apply(selectStatement.getTimeSeriesFunction(), expectedResultSet);
//            log.info("apply cost {} ms", timeCost.end().getCost());
        }

        // 验证结果
        int rowCount = 0;
        boolean hasNullValue = false;
        for (int i = 0; i < fetchColumns.size() - 1; i++) {
            String columnName = TDengineConstantString.REF.getName() + i;
            int columnIndex = result.findColumn(columnName);
            TDengineConstant constant = getConstantFromResultSet(result, columnIndex);

            // 第一行仅含一位数时, STDDEV等聚合函数会返回0值, 0值跳过第一轮比较运算
            if (!constant.isNull()) {
                if (!resultSet.containsKey(timestamp)) {
                    log.info("预期结果集中不包含实际结果集时间戳, timestamp:{} actualValue:{} 聚合结果集:{} 原结果集:{}",
                            timestamp, constant.getBigDecimalValue(), resultSet, expectedResultSet);
                    logAggregationSet(resultSet);
                    return false;
                }
                TDengineConstant isEquals = constant.isEquals(
                        TDengineConstant.createBigDecimalConstant(resultSet.get(timestamp).get(i)));

                if (!isEquals.asBooleanNotNull()) {
                    log.info("预期结果集和实际结果集具体时间戳下数据异常, timestamp:{}, expectedValue:{}, actualValue:{}",
                            timestamp, resultSet.get(timestamp), constant.getBigDecimalValue());
                    logAggregationSet(resultSet);
                    return false;
                }
            } else hasNullValue = true;
        }
        if (!hasNullValue) rowCount++;

        boolean verifyRes = verifyGeneralQuery(resultSet, result, rowCount);
        if (!verifyRes) logAggregationSet(resultSet);
        return verifyRes;
    }

    private boolean verifyGeneralQuery(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result, int rowCount)
            throws Exception {
        // 针对每行进行验证
        while (result.hasNext()) {
            VerifyResultState verifyResult = verifyRowResult(expectedResultSet, result);
            if (verifyResult == VerifyResultState.FAIL) return false;
            else if (verifyResult == VerifyResultState.SUCCESS) rowCount++;
        }
        if (expectedResultSet.size() != rowCount) {
            log.error("预期结果集和实际结果集数目不一致, expectedResultSet:{} resultSet:{}",
                    expectedResultSet.size(), rowCount);
            return false;
        }
        return true;
    }

    private VerifyResultState verifyRowResult(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result) throws Exception {
        // time取最后一列
        TDengineSchema.TDengineColumn timeColumn =
                ((TDengineColumnReference) fetchColumns.get(fetchColumns.size() - 1)).getColumn();
        int timeColumnIndex = result.findColumn(timeColumn.getName().toLowerCase());
        TDengineConstant timeConstant = getConstantFromResultSet(result, timeColumnIndex);
        long timestamp = timeConstant.getBigDecimalValue().longValue();

        for (int i = 0; i < fetchColumns.size() - 1; i++) {
            String columnName = TDengineConstantString.REF.getName() + i;
            int columnIndex = result.findColumn(columnName);
            TDengineConstant constant = getConstantFromResultSet(result, columnIndex);

            // 空值不进行评估
            if (constant.isNull()) return VerifyResultState.IS_NULL;

            if (!expectedResultSet.containsKey(timestamp)) {
                log.info("预期结果集中不包含实际结果集时间戳, timestamp:{}", timestamp);
                return VerifyResultState.FAIL;
            }
            TDengineConstant isEquals = constant.isEquals(
                    new TDengineConstant.TDengineBigDecimalConstant(expectedResultSet.get(timestamp).get(i)));
            if (isEquals.isNull()) throw new AssertionError();
            else if (!isEquals.asBooleanNotNull()) {
                log.info("预期结果集和实际结果集具体时间戳下数据异常, timestamp:{}, expectedValue:{}, actualValue:{}",
                        timestamp, expectedResultSet.get(timestamp), constant.getBigDecimalValue());
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

    private TDengineConstant getConstantFromResultSet(DBValResultSet resultSet, int columnIndex) throws Exception {
        TDengineSchema.TDengineDataType dataType = TDengineSchema.TDengineDataType.getDataTypeByName(
                resultSet.getMetaData().getColumnTypeName(columnIndex));

        TDengineConstant constant;
        if (resultSet.getString(columnIndex) == null
                || resultSet.getString(columnIndex).equalsIgnoreCase("null")) {
            return TDengineConstant.createNullConstant();
        } else {
            Object value;
            switch (dataType) {
                case TIMESTAMP:
                case INT:
                case UINT:
                case UBIGINT:
                case BIGINT:
                    value = resultSet.getLong(columnIndex);
                    constant = TDengineConstant.createBigDecimalConstant(new BigDecimal(String.valueOf(value)));
                    break;
                case BINARY:
                case VARCHAR:
                    value = resultSet.getString(columnIndex);
                    constant = TDengineConstant.createStringConstant((String) value);
                    break;
                case DOUBLE:
                    value = resultSet.getDouble(columnIndex);
                    constant = TDengineConstant.createBigDecimalConstant(new BigDecimal(String.valueOf(value)));
                    break;
                default:
                    throw new AssertionError(dataType);
            }
        }
        if (selectStatement.getAggregationType() != null
                && TDengineAggregationType.getTDengineAggregationType(selectStatement.getAggregationType()).isFillZero()
                && constant.getBigDecimalValue().compareTo(BigDecimal.ZERO) == 0
                && dataType != TDengineSchema.TDengineDataType.TIMESTAMP)
            return TDengineConstant.createNullConstant();
        return constant;
    }

    private TDengineExpression generateExpression(List<TDengineColumn> columns) {
        TDengineExpression result = null;
        boolean reGenerateExpr;
        int regenerateCounter = 0;
        String predicateSequence = "";
        do {
            reGenerateExpr = false;
            try {
                TDengineExpression predicateExpression =
                        new TDengineExpressionGenerator(globalState).setColumns(columns).generateExpression();
                // 将表达式纠正为BOOLEAN类型
                TDengineExpression rectifiedPredicateExpression = predicateExpression;

                // 单列谓词 -> 重新生成
                if (!predicateExpression.getExpectedValue().isBoolean()) {
                    rectifiedPredicateExpression =
                            TDengineUnaryNotPrefixOperation.getNotUnaryPrefixOperation(predicateExpression);
                }

                // add time column
                TDengineExpression timeExpression = new TDengineTimeExpressionGenerator(globalState).setColumns(
                        Collections.singletonList(new TDengineColumn(TDengineConstantString.TIME_FIELD_NAME.getName(),
                                false, TDengineSchema.TDengineDataType.TIMESTAMP))).generateExpression();

                result = new TDengineBinaryLogicalOperation(rectifiedPredicateExpression, timeExpression,
                        TDengineBinaryLogicalOperation.TDengineBinaryLogicalOperator.AND);
//                log.info("Expression: {}", TDengineVisitor.asString(result));

                predicateSequence = TDengineVisitor.asString(rectifiedPredicateExpression, true);
                if (globalState.getOptions().useSyntaxSequence()
                        && TDengineQuerySynthesisFeedbackManager.isRegenerateSequence(predicateSequence)) {
                    regenerateCounter++;
                    if (regenerateCounter >= TDengineQuerySynthesisFeedbackManager.MAX_REGENERATE_COUNT_PER_ROUND) {
                        TDengineQuerySynthesisFeedbackManager.incrementExpressionDepth(
                                globalState.getOptions().getMaxExpressionDepth());
                        regenerateCounter = 0;
                    }
                    throw new ReGenerateExpressionException(String.format("该语法节点序列需重新生成:%s", predicateSequence));
                }
                // 更新概率表
                TDengineQuerySynthesisFeedbackManager.addSequenceRegenerateProbability(predicateSequence);
            } catch (ReGenerateExpressionException e) {
                reGenerateExpr = true;
            }
        } while (reGenerateExpr);

        timePredicate = result;
        this.predicateSequence = predicateSequence;
        return result;
    }

    private void generateTimeWindowClause(List<TDengineColumn> columns) {
        selectStatement.setAggregationType(TDengineAggregationType.getRandomAggregationType());

        String timeUnit = "s";
        long intervalVal = globalState.getRandomly().getLong(1, 10000);
        long intervalOffset = globalState.getRandomly().getLong(0, intervalVal);
        long slidingVal = globalState.getRandomly().getLong(intervalVal / 100 + 1, intervalVal);

        List<String> intervals = new ArrayList<>();
        intervals.add(String.format(String.format("%d%s", intervalVal, timeUnit)));
        if (intervalOffset != 0) intervals.add(String.format(String.format("%d%s", intervalOffset, timeUnit)));
        selectStatement.setIntervalValues(intervals);
        if (slidingVal != 0) selectStatement.setSlidingValue(String.format("%d%s", slidingVal, timeUnit));
    }

    private List<TDengineExpression> generateGroupByClause(List<TDengineColumn> columns, TDengineRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> TDengineColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private TDengineConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return TDengineConstant.createInt32Constant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private TDengineExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return TDengineConstant.createIntConstantNotAsBoolean(0);
        } else {
            return null;
        }
    }

    private String generateSelectClause(TDengineTimeSeriesTemplate template) {
        StringBuilder sb = new StringBuilder();
        sb.append(template.getTemplate().getTemplateString());

        // 依据查询模板选择赋值
        template.genRandomTemplateValues(table, globalState);

        // 替换模板占位符
        for (int i = 0; i < template.getValues().size(); i++) {
            Object value = template.getValues().get(i);
            int index = sb.indexOf("?");
            if (index != -1) sb.replace(index, index + 1, value.toString());
        }
        return sb.toString();
    }
}
