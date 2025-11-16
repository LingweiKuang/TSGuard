package com.fuzzy.victoriametrics;


import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.victoriametrics.ast.*;

import java.util.List;
import java.util.Set;

public interface VMVisitor {

    void visit(VMTableReference ref);

    void visit(VMSchemaReference ref);

    void visit(VMConstant constant);

    void visit(VMColumnReference column);

    void visit(VMBinaryLogicalOperation op);

    void visit(VMSelect select);

    void visit(VMBinaryComparisonOperation op);

//    void visit(VMCastOperation op);

//    void visit(VMBinaryOperation op);

    void visit(VMUnaryPrefixOperation op);

//    void visit(VMUnaryNotPrefixOperation op);

    void visit(VMBinaryArithmeticOperation op);

//    void visit(VMOrderByTerm op);

//    void visit(VMUnaryPostfixOperation op);

//    void visit(VMInOperation op);

//    void visit(VMBetweenOperation op);

//    void visit(VMExists op);
//
//    void visit(VMStringExpression op);

//    void visit(VMComputableFunction f);

    // void visit(VMCollate collate);

    default void visit(VMExpression expr) {
        if (expr instanceof VMConstant) {
            visit((VMConstant) expr);
        } else if (expr instanceof VMColumnReference) {
            visit((VMColumnReference) expr);
        } else if (expr instanceof VMBinaryLogicalOperation) {
            visit((VMBinaryLogicalOperation) expr);
        } else if (expr instanceof VMSelect) {
            visit((VMSelect) expr);
        } else if (expr instanceof VMBinaryComparisonOperation) {
            visit((VMBinaryComparisonOperation) expr);
        } else if (expr instanceof VMBinaryArithmeticOperation) {
            visit((VMBinaryArithmeticOperation) expr);
        } else if (expr instanceof VMTableReference) {
            visit((VMTableReference) expr);
        } else if (expr instanceof VMSchemaReference) {
            visit((VMSchemaReference) expr);
        } else if (expr instanceof VMUnaryPrefixOperation) {
            visit((VMUnaryPrefixOperation) expr);
        } /*else if (expr instanceof VMCastOperation) {
            visit((VMCastOperation) expr);
        } else if (expr instanceof VMOrderByTerm) {
            visit((VMOrderByTerm) expr);
        } else if (expr instanceof VMUnaryPostfixOperation) {
            visit((VMUnaryPostfixOperation) expr);
        } else if (expr instanceof VMBinaryOperation) {
            visit((VMBinaryOperation) expr);
        } else if (expr instanceof VMExists) {
            visit((VMExists) expr);
        } else if (expr instanceof VMInOperation) {
            visit((VMInOperation) expr);
        } else if (expr instanceof VMBetweenOperation) {
            visit((VMBetweenOperation) expr);
        } else if (expr instanceof VMUnaryNotPrefixOperation) {
            visit((VMUnaryNotPrefixOperation) expr);
        } else if (expr instanceof VMComputableFunction) {
            visit((VMComputableFunction) expr);
        } */ else {
            throw new AssertionError(expr);
        }
    }

    static String asString(VMExpression expr) {
        VMToStringVisitor visitor = new VMToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asString(VMExpression expr, boolean isAbstractExpression) {
        VMToStringVisitor visitor = new VMToStringVisitor(isAbstractExpression);
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(VMExpression expr) {
        VMExpectedValueVisitor visitor = new VMExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static TimeSeriesStream streamComputeTimeSeriesVector(String databaseName,
                                                          List<String> fetchColumnNames, Long startTimestamp,
                                                          Long endTimestamp,
                                                          VMExpression expr, Set<Long> nullValuesSet) {
        // 获取时序约束，将所有约束全部转为针对时间戳的限制
        VMStreamComputingVisitor visitor = new VMStreamComputingVisitor(databaseName, fetchColumnNames,
                startTimestamp, endTimestamp, nullValuesSet);
        visitor.visit(expr);
        TimeSeriesStream timeSeriesStream = visitor.timeSeriesStreamStack.pop();
        // TODO 存在列运算, 进行空值过滤(时间戳)
        return timeSeriesStream;
    }

}
