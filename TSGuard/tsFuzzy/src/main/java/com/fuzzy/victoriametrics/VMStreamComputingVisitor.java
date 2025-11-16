package com.fuzzy.victoriametrics;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.streamprocessing.StreamGeneration;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.common.visitor.ToStringVisitor;
import com.fuzzy.common.visitor.UnaryOperation;
import com.fuzzy.victoriametrics.ast.*;
import com.fuzzy.victoriametrics.streamcomputing.VMTimeSeriesVector;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Stack;

@Slf4j
public class VMStreamComputingVisitor extends ToStringVisitor<VMExpression> implements VMVisitor {

    Stack<TimeSeriesStream> timeSeriesStreamStack = new Stack<>();
    //    TimeSeriesConstraint nullValueTimestamps;
    String databaseName;
    List<String> fetchColumnNames;
    Long startTimestamp;
    Long endTimestamp;

    public VMStreamComputingVisitor(String databaseName, List<String> fetchColumnNames,
                                    Long startTimestamp, Long endTimestamp, Set<Long> nullValuesSet) {
        this.databaseName = databaseName;
        this.fetchColumnNames = fetchColumnNames;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;

        // TODO VM 空值去除
//        TimeSeriesConstraint nullValueTimestamps = new TimeSeriesConstraint(VMConstantString.TIME_FIELD_NAME.getName(),
//                new RangeConstraint());
//        TimeSeriesConstraint nullValueTimestamps = new TimeSeriesConstraint("time", new RangeConstraint());
//        nullValueTimestamps.getRangeConstraints().clear();
//        nullValuesSet.forEach(timestamp -> nullValueTimestamps.addEqualValue(new BigDecimal(timestamp)));
//        this.nullValueTimestamps = nullValueTimestamps;
    }

    @Override
    public void visitSpecific(VMExpression expr) {
        VMVisitor.super.visit(expr);
    }

    @Override
    public void visit(VMTableReference ref) {

    }

    @Override
    public void visit(VMSchemaReference ref) {

    }

    @Override
    public void visit(VMConstant constant) {
        TimeSeriesStream timeSeriesStream = new TimeSeriesStream.TimeSeriesScalar(
                new BigDecimal(constant.getTextRepresentation()));
        timeSeriesStreamStack.add(timeSeriesStream);
    }

    @Override
    public void visit(VMColumnReference timeSeries) {
        String tableName = timeSeries.getColumn().getTable().getName();
        TimeSeriesStream.TimeSeriesVector timeSeriesVector = StreamGeneration.genVectorFromSampling(databaseName, tableName,
                Collections.singletonList(timeSeries.getColumn().getName()),
                startTimestamp, endTimestamp,
                VMConstant.createFloatArithmeticTolerance());
        timeSeriesStreamStack.add(timeSeriesVector);
    }

    @Override
    public void visit(UnaryOperation<VMExpression> unaryOperation) {
        if (unaryOperation instanceof VMUnaryPrefixOperation)
            visit((VMUnaryPrefixOperation) unaryOperation);
    }

    @Override
    public void visit(VMBinaryLogicalOperation op) {
        visit(op.getLeft());
        visit(op.getRight());
        // 将结果存储, 弹出运算符
        TimeSeriesStream right = timeSeriesStreamStack.pop();
        TimeSeriesStream left = timeSeriesStreamStack.pop();

        // 仅在瞬时向量间定义: vector op vector
        if (!(left.isVector() && right.isVector())) {
            log.error("set operator \"and\" not allowed in binary scalar expression, left={}, right={}", left, right);
            throw new IgnoreMeException();
        }

        VMTimeSeriesVector leftVector = new VMTimeSeriesVector((TimeSeriesStream.TimeSeriesVector) left);
        VMTimeSeriesVector rightVector = new VMTimeSeriesVector((TimeSeriesStream.TimeSeriesVector) right);
        VMBinaryLogicalOperation.VMBinaryLogicalOperator operator = op.getOp();
        TimeSeriesStream.TimeSeriesVector timeSeriesVectorRes = switch (operator) {
            case AND -> leftVector.and(rightVector);
            case OR -> leftVector.or(rightVector);
            case UNLESS -> leftVector.unless(rightVector);
            default -> throw new AssertionError();
        };
        timeSeriesStreamStack.add(timeSeriesVectorRes);
    }

    @Override
    public void visit(VMSelect s) {

    }

    @Override
    public void visit(VMBinaryComparisonOperation op) {
        visit(op.getLeft());
        visit(op.getRight());
        TimeSeriesStream right = timeSeriesStreamStack.pop();
        TimeSeriesStream left = timeSeriesStreamStack.pop();

        // 标量/标量、向量/标量 以及 向量/向量
        VMBinaryComparisonOperation.BinaryComparisonOperator operator = op.getOp();
        TimeSeriesStream timeSeriesStreamResult = switch (operator) {
            case EQUALS -> left.equal(right);
            case NOT_EQUALS -> left.notEqual(right);
            case GREATER -> left.greaterThan(right);
            case GREATER_EQUALS -> left.greaterOrEqual(right);
            case LESS -> left.lessThan(right);
            case LESS_EQUALS -> left.lessOrEqual(right);
            default -> throw new AssertionError();
        };
        timeSeriesStreamStack.add(timeSeriesStreamResult);
    }

    @Override
    public void visit(VMUnaryPrefixOperation op) {
        visit(op.getExpression());
        TimeSeriesStream timeSeriesStream = timeSeriesStreamStack.pop();
        // 约束条件取反
        if (op.getOp() == VMUnaryPrefixOperation.VMUnaryPrefixOperator.MINUS)
            timeSeriesStreamStack.add(timeSeriesStream.negate());
        else
            timeSeriesStreamStack.add(timeSeriesStream);
    }

    @Override
    public void visit(VMBinaryArithmeticOperation op) {
        visit(op.getLeft());
        visit(op.getRight());
        TimeSeriesStream right = timeSeriesStreamStack.pop();
        TimeSeriesStream left = timeSeriesStreamStack.pop();

        VMBinaryArithmeticOperation.VMBinaryArithmeticOperator operator = op.getOp();
        TimeSeriesStream timeSeriesStreamRes = switch (operator) {
            case PLUS -> left.add(right);
            case SUBTRACT -> left.subtract(right);
            case MULTIPLY -> left.multiply(right);
            case DIVIDE -> left.divide(right);
            case ATAN2 -> left.atan2(right);
            case MODULO -> {
                log.warn("暂时不支持MODULO");
                throw new IgnoreMeException();
            }
            default -> throw new AssertionError();
        };

        timeSeriesStreamStack.add(timeSeriesStreamRes);
    }

//    @Override
//    public void visit(VMUnaryNotPrefixOperation op) {
//        // TODO 暂时不实现 NOT 前缀表达
//        // Unary NOT Prefix Operation
//        visit(op.getExpression());
//        TimeSeriesStream timeSeriesStream = timeSeriesStreamStack.pop();
//        if (timeSeriesStream.isVector()) {
//            // 向量 NOT vector => vector == expectedValue(第一个点的数据)
//            BigDecimal expectedValue = op.getExpression().getExpectedValue().castAs(
//                    VMSchema.CommonDataType.BIGDECIMAL).getBigDecimalValue();
//
//            // TODO 筛出满足条件的 vector
//            timeSeriesStreamStack.add(timeSeriesStream);
//        } else if (timeSeriesStream.isScalar()) {
//            // 标量维持不变 scalar == scalar
//            timeSeriesStreamStack.add(new TimeSeriesStream.TimeSeriesScalar(BigDecimal.ONE));
//        } else {
//            throw new AssertionError();
//        }
//    }

//    @Override
//    public void visit(VMOrderByTerm op) {
//        // TODO
//    }

//    @Override
//    public void visit(VMUnaryPostfixOperation op) {
//        visit(op.getExpression());
//        ConstraintValue exprValue = timeSeriesVectorStack.pop();
//
//        if (exprValue.isConstant() && !op.isNegated())
//            timeSeriesVectorStack.add(ConstraintValueGenerator.genFalseConstraint());
//        else if (exprValue.isConstant() && op.isNegated())
//            timeSeriesVectorStack.add(ConstraintValueGenerator.genTrueConstraint());
//        else if (exprValue.isTimeSeries() && !op.isNegated()) {
//            // timeSeriesValue is null -> timestamp list
//            timeSeriesVectorStack.add(ConstraintValueGenerator.genConstraint(
//                    ConstraintValueGenerator.genTimeSeries(VMConstantString.TIME_FIELD_NAME.getName()),
//                    nullValueTimestamps));
//        } else if (exprValue.isTimeSeries() && op.isNegated()) {
//            // 将timeSeriesConstraint因子截距设为0
//            TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint(exprValue.getTimeSeriesName(),
//                    new RangeConstraint());
//            timeSeriesVectorStack.add(ConstraintValueGenerator.genConstraint(exprValue.getTimeSeriesValue(),
//                    timeSeriesConstraint));
//        } else if (exprValue.isTimeSeriesConstraint() && !op.isNegated()) {
//            TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint(GlobalConstant.BASE_TIME_SERIES_NAME,
//                    new RangeConstraint());
//            timeSeriesConstraint.getRangeConstraints().clear();
//            timeSeriesVectorStack.add(ConstraintValueGenerator.genConstraint(ConstraintValueGenerator.genBaseTimeSeries(),
//                    timeSeriesConstraint));
//        } else if (exprValue.isTimeSeriesConstraint() && op.isNegated()) {
//            timeSeriesVectorStack.add(ConstraintValueGenerator.genTrueConstraint());
//        } else {
//            throw new AssertionError();
//        }
//    }
//
//    @Override
//    public void visit(VMInOperation op) {
//        if (op.getListElements().isEmpty()) throw new AssertionError();
//
//        // 将IN转换为OR表达式组合
//        VMBinaryComparisonOperation.BinaryComparisonOperator comparisonOperator;
//        VMBinaryLogicalOperation.VMBinaryLogicalOperator logicalOperator;
//        if (!op.isTrue()) {
//            comparisonOperator = VMBinaryComparisonOperation.BinaryComparisonOperator.NOT_EQUALS;
//            logicalOperator = VMBinaryLogicalOperation.VMBinaryLogicalOperator.AND;
//        } else {
//            comparisonOperator = VMBinaryComparisonOperation.BinaryComparisonOperator.EQUALS;
//            logicalOperator = VMBinaryLogicalOperation.VMBinaryLogicalOperator.OR;
//        }
//
//        int index = 0;
//        VMExpression combinationExpr = new VMBinaryComparisonOperation(op.getExpr(),
//                op.getListElements().get(index++), comparisonOperator);
//
//        for (; index < op.getListElements().size(); index++) {
//            VMBinaryComparisonOperation right = new VMBinaryComparisonOperation(op.getExpr(),
//                    op.getListElements().get(index), comparisonOperator);
//            combinationExpr = new VMBinaryLogicalOperation(combinationExpr, right, logicalOperator);
//        }
//        visit(combinationExpr);
//    }
//
//    @Override
//    public void visit(VMBetweenOperation op) {
//        // 将BETWEEN转换为LESS_EQUALS
//        VMBinaryComparisonOperation left = new VMBinaryComparisonOperation(op.getLeft(), op.getExpr(),
//                VMBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
//        VMBinaryComparisonOperation right = new VMBinaryComparisonOperation(op.getExpr(), op.getRight(),
//                VMBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
//        visit(new VMBinaryLogicalOperation(left, right,
//                VMBinaryLogicalOperation.VMBinaryLogicalOperator.AND));
//    }
//
//    @Override
//    public void visit(VMComputableFunction f) {
//        visit(f.getExpectedValue());
//    }
}
