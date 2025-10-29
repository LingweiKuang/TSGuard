package com.fuzzy.common.oracle;


import cn.hutool.core.collection.ConcurrentHashSet;
import com.alibaba.fastjson.JSONObject;
import com.benchmark.entity.DBValResultSet;
import com.fuzzy.GlobalState;
import com.fuzzy.IgnoreMeException;
import com.fuzzy.TSFuzzyDBConnection;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.common.tsaf.Equations;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public abstract class TimeSeriesStreamComputingBase<S extends GlobalState<?, ?, C>, E, C extends TSFuzzyDBConnection>
        implements TestOracle<S> {

    protected final ExpectedErrors errors = new ExpectedErrors();

    // 时间谓词 -> 筛选数据
    protected E predicate = null;
    protected String predicateSequence = "";
    protected static final ConcurrentHashSet<String> pivotRowAbstractPredicates = new ConcurrentHashSet<>();

    protected final S globalState;

    protected TimeSeriesStreamComputingBase(S globalState) {
        this.globalState = globalState;
    }

    @Override
    public final void check() throws Exception {
        Query<C> timeseriesQuery = getTimeSeriesQuery();
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(timeseriesQuery.getQueryString());
        }

        boolean pivotRowIsContained = containsRows(timeseriesQuery);
        if (!pivotRowIsContained) {
            reportMissingPivotRow(timeseriesQuery);
        } else {
            // statistical
            incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.success);
            log.info("有效查询: {}", timeseriesQuery.getQueryString());
        }
    }

    private boolean containsRows(Query<C> query) throws Exception {
        // 验证预期结果集和实际结果集等价性
        try (DBValResultSet result = query.executeAndGet(globalState)) {
            if (result == null) {
                globalState.getLogger().writeSyntaxErrorQuery(String.format("无效查询: %s;", query.getQueryString()));
                // 将无效查询序列重生成概率设为10
                setSequenceRegenerateProbabilityToMax(predicateSequence);
                // statistical
                incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.invalid);
                throw new IgnoreMeException();
            }

            // 流式计算获取预期结果集
            TimeSeriesStream expectedResultSet = null;
            try {
                expectedResultSet = getExpectedValues(predicate);
            } catch (IgnoreMeException e) {
                // TODO StreamComputing 暂不支持该复杂表达式运算, 将其命中统计为success
                incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.success);
                throw e;
            }
            // 验证结果集
            return verifyResultSet(expectedResultSet, result);
        }
    }

    protected void reportMissingPivotRow(Query<?> query) {
        // statistical
        incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.error);

        StringBuilder sb = new StringBuilder();
        if (predicate != null) {
            TimeSeriesStream expectedValues = getExpectedValues(predicate);
            int size = expectedValues.isScalar() ? 1 :
                    ((TimeSeriesStream.TimeSeriesVector) expectedValues).getElements().size();
            // 流数据数目
            sb.append("--\n-- time predicates and their expected values:\n")
                    .append("-- size: ")
                    .append(size)
                    .append("-- \n");
            // 时间序列流
            sb.append("--\n-- TimeSeriesStream:\n")
                    .append("-- ")
                    .append(JSONObject.toJSONString(expectedValues));
            // 生成数据方程式
            List<Equations> equations =
                    EquationsManager.getInstance().getAllEquationsFromDatabase(globalState.getDatabaseName());
            sb.append("--\n-- Equations Params:\n")
                    .append("-- ")
                    .append(JSONObject.toJSONString(equations));
            // 采样频率
            List<SamplingFrequency> samplingFrequencies =
                    SamplingFrequencyManager.getInstance().getAllSamplingFrequencyFromCollection(globalState.getDatabaseName());
            sb.append("--\n-- Sampling Frequency:\n")
                    .append("-- ")
                    .append(JSONObject.toJSONString(samplingFrequencies));
            // 表达式抽象结构
            pivotRowAbstractPredicates.forEach(pivotRow -> {
                if (pivotRow.contains(predicateSequence)) {
                    // 探索空间：表达式递归深度逐级递增
                    // => 从小序列搜集到大序列, 但凡存在包含关系的pivotRowAbstractPredicates均忽略重复报错
                    System.out.println("序列报错重复");
                }
            });
            sb.append("--\n-- time predicates abstract expression:\n")
                    .append(predicateSequence)
                    .append("\n--");
            pivotRowAbstractPredicates.add(predicateSequence);
            // 将出错序列重生成概率设为10
            setSequenceRegenerateProbabilityToMax(predicateSequence);
        }
        globalState.getState().getLocalState().log(sb.toString());
        throw new AssertionError(query);
    }

    protected abstract Query<C> getTimeSeriesQuery() throws Exception;

    protected abstract void setSequenceRegenerateProbabilityToMax(String sequence);

    protected abstract void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType);

    protected abstract TimeSeriesStream getExpectedValues(E expr);

    protected abstract boolean verifyResultSet(TimeSeriesStream expectedResultSet, DBValResultSet result);

}
