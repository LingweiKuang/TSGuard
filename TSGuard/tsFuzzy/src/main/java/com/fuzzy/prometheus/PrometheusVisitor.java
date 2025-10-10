package com.fuzzy.prometheus;


import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.common.tsaf.RangeConstraint;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.prometheus.ast.*;

import java.util.List;
import java.util.Set;

public interface PrometheusVisitor {

    void visit(PrometheusTableReference ref);

    void visit(PrometheusSchemaReference ref);

    void visit(PrometheusConstant constant);

    void visit(PrometheusColumnReference column);

    void visit(PrometheusBinaryLogicalOperation op);

    void visit(PrometheusSelect select);

    void visit(PrometheusBinaryComparisonOperation op);

//    void visit(PrometheusCastOperation op);

//    void visit(PrometheusBinaryOperation op);

    void visit(PrometheusUnaryPrefixOperation op);

    void visit(PrometheusUnaryNotPrefixOperation op);

    void visit(PrometheusBinaryArithmeticOperation op);

//    void visit(PrometheusOrderByTerm op);

//    void visit(PrometheusUnaryPostfixOperation op);

//    void visit(PrometheusInOperation op);

//    void visit(PrometheusBetweenOperation op);

//    void visit(PrometheusExists op);
//
//    void visit(PrometheusStringExpression op);

//    void visit(PrometheusComputableFunction f);

    // void visit(PrometheusCollate collate);

    default void visit(PrometheusExpression expr) {
        if (expr instanceof PrometheusConstant) {
            visit((PrometheusConstant) expr);
        } else if (expr instanceof PrometheusColumnReference) {
            visit((PrometheusColumnReference) expr);
        } else if (expr instanceof PrometheusBinaryLogicalOperation) {
            visit((PrometheusBinaryLogicalOperation) expr);
        } else if (expr instanceof PrometheusSelect) {
            visit((PrometheusSelect) expr);
        } else if (expr instanceof PrometheusBinaryComparisonOperation) {
            visit((PrometheusBinaryComparisonOperation) expr);
        } else if (expr instanceof PrometheusBinaryArithmeticOperation) {
            visit((PrometheusBinaryArithmeticOperation) expr);
        } else if (expr instanceof PrometheusTableReference) {
            visit((PrometheusTableReference) expr);
        } else if (expr instanceof PrometheusSchemaReference) {
            visit((PrometheusSchemaReference) expr);
        } else if (expr instanceof PrometheusUnaryPrefixOperation) {
            visit((PrometheusUnaryPrefixOperation) expr);
        } /*else if (expr instanceof PrometheusCastOperation) {
            visit((PrometheusCastOperation) expr);
        } else if (expr instanceof PrometheusOrderByTerm) {
            visit((PrometheusOrderByTerm) expr);
        } else if (expr instanceof PrometheusUnaryPostfixOperation) {
            visit((PrometheusUnaryPostfixOperation) expr);
        } else if (expr instanceof PrometheusBinaryOperation) {
            visit((PrometheusBinaryOperation) expr);
        } else if (expr instanceof PrometheusExists) {
            visit((PrometheusExists) expr);
        } else if (expr instanceof PrometheusInOperation) {
            visit((PrometheusInOperation) expr);
        } else if (expr instanceof PrometheusBetweenOperation) {
            visit((PrometheusBetweenOperation) expr);
        } else if (expr instanceof PrometheusUnaryNotPrefixOperation) {
            visit((PrometheusUnaryNotPrefixOperation) expr);
        } else if (expr instanceof PrometheusComputableFunction) {
            visit((PrometheusComputableFunction) expr);
        } */ else {
            throw new AssertionError(expr);
        }
    }

    static String asString(PrometheusExpression expr) {
        PrometheusToStringVisitor visitor = new PrometheusToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asString(PrometheusExpression expr, boolean isAbstractExpression) {
        PrometheusToStringVisitor visitor = new PrometheusToStringVisitor(isAbstractExpression);
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(PrometheusExpression expr) {
        PrometheusExpectedValueVisitor visitor = new PrometheusExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static TimeSeriesConstraint asConstraint(String databaseName, String tableName,
                                             PrometheusExpression expr, Set<Long> nullValuesSet) {
        // 获取时序约束，将所有约束全部转为针对时间戳的限制
        PrometheusToConstraintVisitor visitor = new PrometheusToConstraintVisitor(databaseName, tableName, nullValuesSet);
        visitor.visit(expr);
        TimeSeriesConstraint timeSeriesConstraint = visitor.constraintStack.pop().getTimeSeriesConstraint();
//        if (!expr.hasColumn()) return columnConstraint;
        // 存在列运算, 进行空值过滤(时间戳)
        TimeSeriesConstraint nullValueFilter = new TimeSeriesConstraint(GlobalConstant.BASE_TIME_SERIES_NAME,
                new RangeConstraint());
        nullValuesSet.forEach(timestamp -> nullValueFilter.addNotEqualValue(SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection(databaseName, tableName).getSeqByTimestamp(timestamp)));
        return timeSeriesConstraint.intersects(nullValueFilter);
    }

    static TimeSeriesStream streamComputeTimeSeriesVector(String databaseName,
                                                          List<String> fetchColumnNames, Long startTimestamp,
                                                          PrometheusExpression expr, Set<Long> nullValuesSet) {
        // 获取时序约束，将所有约束全部转为针对时间戳的限制
        PrometheusStreamComputingVisitor visitor = new PrometheusStreamComputingVisitor(databaseName, fetchColumnNames,
                startTimestamp, nullValuesSet);
        visitor.visit(expr);
        TimeSeriesStream timeSeriesStream = visitor.timeSeriesStreamStack.pop();
        // TODO 存在列运算, 进行空值过滤(时间戳)
        return timeSeriesStream;
    }

}
