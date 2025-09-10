package com.fuzzy.iotdb.oracle;


import com.benchmark.entity.DBValResultSet;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
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
import com.fuzzy.common.tsaf.util.GenerateTimestampUtil;
import com.fuzzy.common.util.TimeCost;
import com.fuzzy.iotdb.IotDBErrors;
import com.fuzzy.iotdb.IotDBGlobalState;
import com.fuzzy.iotdb.IotDBSchema;
import com.fuzzy.iotdb.IotDBSchema.*;
import com.fuzzy.iotdb.IotDBVisitor;
import com.fuzzy.iotdb.ast.*;
import com.fuzzy.iotdb.feedback.IotDBQuerySynthesisFeedbackManager;
import com.fuzzy.iotdb.gen.IotDBExpressionGenerator;
import com.fuzzy.iotdb.gen.IotDBInsertGenerator;
import com.fuzzy.iotdb.gen.IotDBTimeExpressionGenerator;
import com.fuzzy.iotdb.resultSet.IotDBResultSet;
import com.fuzzy.iotdb.tsaf.IotDBAggregationType;
import com.fuzzy.iotdb.tsaf.IotDBTimeSeriesFunc;
import com.fuzzy.iotdb.util.IotDBValueStateConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class IotDBTSAFOracle
        extends TimeSeriesAlgebraFrameworkBase<IotDBGlobalState, IotDBExpression, SQLConnection> {

    // TODO 插入数据空间概率分布
    // 数值和时间戳具备二元函数关系（能否知微见著，反映出各种不规律数值空间？）
    // 预期结果集由两者得出
    // TODO 完整插入所有数据后，随机更新某些时间戳数据（或者插入数据时间戳乱序，前者验证lsm树更新操作，后者验证时序数据库的合并排序）
    // TODO （时间戳和数值按列单独存储，所以两者之间的关系性不会影响到时序数据库查询？看目前是否有时序数据库压缩，查询操作考虑到多列相关性）
    private List<IotDBExpression> fetchColumns;
    private List<IotDBColumn> columns;
    private IotDBTable table;
    private IotDBExpression whereClause;
    IotDBSelect selectStatement;

    public IotDBTSAFOracle(IotDBGlobalState globalState) {
        super(globalState);
        IotDBErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getTimeSeriesQuery() {
        selectStatement = new IotDBSelect();
        IotDBSchema schema = globalState.getSchema();
        IotDBTables randomFromTables = schema.getOneRandomTableNonEmptyTables();
        table = Randomly.fromList(randomFromTables.getTables());
        // IotDB不支持多个table置于From clause中，from后接database
        selectStatement.setFromList(new ArrayList<>(Collections.singletonList(new IotDBTableReference(table))));
        // 时间戳字段设置表信息(get(0)) -> 该数据库下任意表均有该字段
        // 注: TSAF和大多数时序数据库均不支持将time字段和其他字段进行比较、算术二元运算
        columns = randomFromTables.getColumns();

//        QueryType queryType = Randomly.fromOptions(QueryType.values());
        QueryType queryType = Randomly.fromOptions(QueryType.BASE_QUERY);
        selectStatement.setQueryType(queryType);
        // 随机窗口查询测试
        if (queryType.isTimeWindowQuery()) generateTimeWindowClause();
        else if (queryType.isTimeSeriesFunction())
            selectStatement.setTimeSeriesFunction(IotDBTimeSeriesFunc.getRandomFunction(
                    columns, globalState.getRandomly()));
        fetchColumns = columns.stream().map(column -> {
            String columnName = column.getName();
            if (queryType.isTimeWindowQuery())
                columnName = String.format("%s(%s)",
                        IotDBAggregationType.getIotDBAggregationType(selectStatement.getAggregationType()), columnName);
            else if (queryType.isTimeSeriesFunction())
                columnName = selectStatement.getTimeSeriesFunction().combinedArgs(columnName);
            return new IotDBColumnReference(new IotDBColumn(columnName, column.isTag(), column.getType()), null);
        }).collect(Collectors.toList());
        selectStatement.setFetchColumns(fetchColumns);
        whereClause = generateExpression(columns);
        selectStatement.setWhereClause(whereClause);

        // ORDER BY
//        List<IotDBExpression> orderBy = new IotDBExpressionGenerator(globalState).setColumns(columns)
//                .generateOrderBys();
//        selectStatement.setOrderByExpressions(orderBy);
        return new SQLQueryAdapter(IotDBVisitor.asString(selectStatement), errors);
    }

    @Override
    protected void setSequenceRegenerateProbabilityToMax(String sequence) {
        IotDBQuerySynthesisFeedbackManager.setSequenceRegenerateProbabilityToMax(sequence);
    }

    @Override
    protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType) {
        switch (queryType) {
            case error:
                IotDBQuerySynthesisFeedbackManager.incrementErrorQueryCount();
                break;
            case invalid:
                IotDBQuerySynthesisFeedbackManager.incrementInvalidQueryCount();
                break;
            case success:
                IotDBQuerySynthesisFeedbackManager.incrementSuccessQueryCount();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Invalid QueryExecutionType: %s", queryType));
        }
    }

    @Override
    protected TimeSeriesConstraint genColumnConstraint(IotDBExpression expr) {
        return IotDBVisitor.asConstraint(globalState.getDatabaseName(), table.getName(), expr,
                TableToNullValuesManager.getNullValues(globalState.getDatabaseName(), table.getName()));
    }

    @Override
    protected Map<Long, List<BigDecimal>> getExpectedValues(IotDBExpression expression) {
        // 联立方程式求解
        TimeCost timeCost = new TimeCost().begin();
        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        List<String> fetchColumnNames = columns.stream().map(AbstractTableColumn::getName).collect(Collectors.toList());
        TimeSeriesConstraint timeSeriesConstraint = IotDBVisitor.asConstraint(databaseName, tableName,
                expression, TableToNullValuesManager.getNullValues(databaseName, tableName));
        PredicationEquation predicationEquation = new PredicationEquation(timeSeriesConstraint, "equationName");
        Map<Long, List<BigDecimal>> resultSet = predicationEquation.genExpectedResultSet(databaseName, tableName, fetchColumnNames,
                globalState.getOptions().getStartTimestampOfTSData(),
                IotDBInsertGenerator.getLastTimestamp(databaseName, tableName),
                IotDBConstant.createFloatArithmeticTolerance());
        IotDBQuerySynthesisFeedbackManager.incrementCalculateTimeOverhead(timeCost.end().getCost());
        return resultSet;
    }

    @Override
    protected boolean verifyResultSet(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result) {
        try {
            if (selectStatement.getQueryType().isTimeSeriesFunction()
                    || selectStatement.getQueryType().isTimeWindowQuery())
                return verifyTimeWindowQuery(expectedResultSet, result);
            else return verifyGeneralQuery(expectedResultSet, result, expectedResultSet.size());
        } catch (Exception e) {
            log.error("验证查询结果集和预期结果集等价性异常, e:", e);
            return false;
        }
    }

    private boolean verifyTimeWindowQuery(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result)
            throws Exception {
        // 对expectedResultSet进行聚合, 聚合结果按照时间戳作为Key, 进行进一步数值比较
        Map<Long, List<BigDecimal>> resultSet = null;

        // 窗口聚合 -> 预期结果集
        int size = 0;
        if (selectStatement.getQueryType().isTimeWindowQuery()) {
            String startTimestampString = selectStatement.getIntervalValues().get(0);
            String endTimestampString = selectStatement.getIntervalValues().get(1);
            String intervalVal = selectStatement.getIntervalValues().get(selectStatement.getIntervalValues().size() - 1);
            String slidingValue = selectStatement.getSlidingValue();
            long startTimestamp = Long.parseLong(startTimestampString);
            long endTimestamp = Long.parseLong(endTimestampString) - 1;
            long duration = globalState.transTimestampToPrecision(
                    Long.parseLong(intervalVal.substring(0, intervalVal.length() - 1)) * 1000);
            long sliding = globalState.transTimestampToPrecision(
                    Long.parseLong(slidingValue.substring(0, slidingValue.length() - 1)) * 1000);
            size = GenerateTimestampUtil.genTimestampsInRange(startTimestamp, endTimestamp, sliding).size();
            resultSet = selectStatement.getAggregationType()
                    .apply(expectedResultSet, startTimestamp, duration, sliding);
        } else if (selectStatement.getQueryType().isTimeSeriesFunction()) {
            resultSet = IotDBTimeSeriesFunc.apply(selectStatement.getTimeSeriesFunction(), expectedResultSet);
            size = resultSet.size();
        }

        // 验证结果
        boolean verifyRes = verifyGeneralQuery(resultSet, result, size);
        if (!verifyRes) logAggregationSet(resultSet);
        return verifyRes;
    }

    private boolean verifyGeneralQuery(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result,
                                       int expectedResultSetSize) throws Exception {
        // 针对每行进行验证
        int rowCount = 0;
        int isNullCount = 0;
        while (result.hasNext()) {
            VerifyResultState verifyResult = verifyRowResult(expectedResultSet, result);
            if (verifyResult == VerifyResultState.FAIL) return false;
            else if (verifyResult == VerifyResultState.IS_NULL) isNullCount++;
            else rowCount++;
        }

        // TODO 若谓词表达式为FALSE, IotDB忽略补充NULL值, 已上报该问题, 将空值数目不一致移除
        if (expectedResultSetSize != rowCount + isNullCount)
            log.warn("预期结果集和实际结果集数目不一致(含空值), expectedResultSetSize:{} rowCount:{} isNullCount:{}",
                    expectedResultSetSize, rowCount, isNullCount);

        if (expectedResultSet.size() != rowCount) {
            log.error("预期结果集和实际结果集数目不一致, expectedResultSetSize:{} rowCount:{} isNullCount:{}",
                    expectedResultSet.size(), rowCount, isNullCount);
            return false;
        } else log.info("空值数目:{}", isNullCount);

        return true;
    }

    private VerifyResultState verifyRowResult(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result)
            throws Exception {
        IotDBResultSet iotDBResultSet = (IotDBResultSet) result;
        for (int i = 0; i < fetchColumns.size(); i++) {
            String columnName = "ref" + i;
            IotDBConstant constant = getConstantFromResultSet(iotDBResultSet, columnName);
            // 空值跳过评估
            if (constant.isNull()) return VerifyResultState.IS_NULL;
            long timestamp = iotDBResultSet.getTimestamp().getTime();

            if (!expectedResultSet.containsKey(timestamp)) {
                log.info("预期结果集中不包含实际结果集时间戳, timestamp:{}", timestamp);
                return VerifyResultState.FAIL;
            }
            IotDBConstant isEquals = constant.isEquals(
                    new IotDBConstant.IotDBBigDecimalConstant(expectedResultSet.get(timestamp).get(i)));
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

    private IotDBConstant getConstantFromResultSet(IotDBResultSet resultSet, String columnName)
            throws Exception {
        int columnIndex = resultSet.findColumn(columnName);

        if (resultSet.valueIsNull(columnIndex)) return IotDBConstant.createNullConstant();

        IotDBDataType dataType = resultSet.getColumnType(columnName);
        IotDBConstant constant;
        if (resultSet.getString(columnIndex) == null) {
            constant = IotDBConstant.createNullConstant();
        } else {
            Object value;
            switch (dataType) {
                case BIGDECIMAL:
                case FLOAT:
                case DOUBLE:
                    value = resultSet.getDouble(columnIndex);
                    constant = IotDBConstant.createBigDecimalConstant(new BigDecimal(String.valueOf(value)));
                    break;
                case INT32:
                    value = resultSet.getInt(columnIndex);
                    constant = IotDBConstant.createBigDecimalConstant(new BigDecimal(String.valueOf(value)));
                    break;
                case INT64:
                    value = resultSet.getLong(columnIndex);
                    constant = IotDBConstant.createBigDecimalConstant(new BigDecimal(String.valueOf(value)));
                    break;
                case BOOLEAN:
                case TEXT:
                case NULL:
                    value = resultSet.getString(columnIndex);
                    constant = IotDBConstant.createStringConstant((String) value);
                    break;
                default:
                    throw new AssertionError(dataType);
            }
        }
        // 空值跳过评估(COUNT补0值, 其他聚合函数补充NULL值)
        if (selectStatement.getAggregationType() != null
                && IotDBAggregationType.getIotDBAggregationType(selectStatement.getAggregationType()).isFillZero()
                && constant.getBigDecimalValue().compareTo(BigDecimal.ZERO) == 0
                && !columnName.equalsIgnoreCase(IotDBValueStateConstant.TIME_FIELD.getValue()))
            return IotDBConstant.createNullConstant();
        return constant;
    }

    private IotDBExpression generateExpression(List<IotDBColumn> columns) {
        IotDBExpression result = null;
        boolean reGenerateExpr;
        int regenerateCounter = 0;
        String predicateSequence = "";
        do {
            reGenerateExpr = false;
            try {
                IotDBExpression predicateExpression =
                        new IotDBExpressionGenerator(globalState).setColumns(columns).generateExpression();
                // 将表达式纠正为BOOLEAN类型
                IotDBExpression rectifiedPredicateExpression = predicateExpression;
                if (!predicateExpression.getExpectedValue().isBoolean()) {
                    rectifiedPredicateExpression =
                            IotDBUnaryNotPrefixOperation.getNotUnaryPrefixOperation(predicateExpression);
                }

                // generate time column
                IotDBColumn timeColumn = new IotDBColumn(IotDBValueStateConstant.TIME_FIELD.getValue(),
                        false, IotDBDataType.INT64);
                IotDBTimeExpressionGenerator timeExpressionGenerator = new IotDBTimeExpressionGenerator(globalState);
                IotDBExpression timeExpression = timeExpressionGenerator.setColumns(
                        Collections.singletonList(timeColumn)).generateExpression();

                if (!ObjectUtils.isEmpty(selectStatement.getIntervalValues())) {
                    // 聚合函数 具备时间范围限制
                    long startTimestamp = Long.parseLong(selectStatement.getIntervalValues().get(0));
                    long endTimestamp = Long.parseLong(selectStatement.getIntervalValues().get(1));
                    IotDBBetweenOperation timeBetween = new IotDBBetweenOperation(
                            timeExpressionGenerator.generateColumn(),
                            IotDBConstant.createInt64Constant(startTimestamp),
                            IotDBConstant.createInt64Constant(endTimestamp), false);

                    timeExpression = new IotDBBinaryLogicalOperation(timeBetween, timeExpression,
                            IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.AND);
                }

                result = new IotDBBinaryLogicalOperation(rectifiedPredicateExpression, timeExpression,
                        IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.AND);

                // 语法节点序列指导查询合成
                predicateSequence = IotDBVisitor.asString(rectifiedPredicateExpression, true);
                if (globalState.getOptions().useSyntaxSequence()
                        && IotDBQuerySynthesisFeedbackManager.isRegenerateSequence(predicateSequence)) {
                    regenerateCounter++;
                    if (regenerateCounter >= IotDBQuerySynthesisFeedbackManager.MAX_REGENERATE_COUNT_PER_ROUND) {
                        IotDBQuerySynthesisFeedbackManager.incrementExpressionDepth(
                                globalState.getOptions().getMaxExpressionDepth());
                        regenerateCounter = 0;
                    }
                    throw new ReGenerateExpressionException(String.format("该语法节点序列需重新生成:%s", predicateSequence));
                }
                // 更新概率表
                IotDBQuerySynthesisFeedbackManager.addSequenceRegenerateProbability(predicateSequence);
            } catch (ReGenerateExpressionException e) {
                log.info("ReGenerateExpression: {}", e.getMessage());
                reGenerateExpr = true;
            }
        } while (reGenerateExpr);

        timePredicate = result;
        this.predicateSequence = predicateSequence;
        return result;
    }

    private void generateTimeWindowClause() {
        selectStatement.setAggregationType(IotDBAggregationType.getRandomAggregationType());

        List<String> intervals = new ArrayList<>();
        // TODO 采样点前后1000s 取决于采样点endStartTimestamp
        long timestampInterval = Randomly.getNotCachedInteger(1000, 3000) * 1000L;
        long startTimestamp = globalState.getRandomly().getLong(
                globalState.getOptions().getStartTimestampOfTSData() - timestampInterval,
                globalState.getOptions().getStartTimestampOfTSData() + timestampInterval);
        long endTimestamp = globalState.getRandomly().getLong(
                globalState.getOptions().getStartTimestampOfTSData() - timestampInterval,
                globalState.getOptions().getStartTimestampOfTSData() + timestampInterval);
        intervals.add(String.valueOf(Math.min(startTimestamp, endTimestamp)));
        intervals.add(String.valueOf(Math.max(startTimestamp, endTimestamp)));
        String timeUnit = "s";
        long windowSize = globalState.getRandomly().getLong(1, timestampInterval / 1000);
        long slidingSize = globalState.getRandomly().getLong(1, timestampInterval / 1000);
        intervals.add(String.format(String.format("%d%s", windowSize, timeUnit)));
        selectStatement.setIntervalValues(intervals);
        selectStatement.setSlidingValue(String.format(String.format("%d%s", slidingSize, timeUnit)));
    }

    private List<IotDBExpression> generateGroupByClause(List<IotDBColumn> columns, IotDBRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> IotDBColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private IotDBConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return IotDBConstant.createInt32Constant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private IotDBExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return IotDBConstant.createIntConstantNotAsBoolean(0);
        } else {
            return null;
        }
    }
}
