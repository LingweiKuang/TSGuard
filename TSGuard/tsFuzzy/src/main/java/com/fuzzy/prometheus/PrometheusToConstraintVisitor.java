package com.fuzzy.prometheus;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.tsaf.ConstraintValue;
import com.fuzzy.common.tsaf.ConstraintValueGenerator;
import com.fuzzy.common.tsaf.RangeConstraint;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import com.fuzzy.common.util.BigDecimalUtil;
import com.fuzzy.common.visitor.ToStringVisitor;
import com.fuzzy.common.visitor.UnaryOperation;
import com.fuzzy.prometheus.ast.*;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Set;
import java.util.Stack;

@Slf4j
public class PrometheusToConstraintVisitor extends ToStringVisitor<PrometheusExpression> implements PrometheusVisitor {

    Stack<ConstraintValue> constraintStack = new Stack<>();
    TimeSeriesConstraint nullValueTimestamps;
    String databaseName;
    String tableName;

    public PrometheusToConstraintVisitor(String databaseName, String tableName, Set<Long> nullValuesSet) {
        this.databaseName = databaseName;
        this.tableName = tableName;

        // TODO Prometheus 空值去除
//        TimeSeriesConstraint nullValueTimestamps = new TimeSeriesConstraint(PrometheusConstantString.TIME_FIELD_NAME.getName(),
//                new RangeConstraint());
        TimeSeriesConstraint nullValueTimestamps = new TimeSeriesConstraint("time", new RangeConstraint());
        nullValueTimestamps.getRangeConstraints().clear();
        nullValuesSet.forEach(timestamp -> nullValueTimestamps.addEqualValue(new BigDecimal(timestamp)));
        this.nullValueTimestamps = nullValueTimestamps;
    }

    @Override
    public void visitSpecific(PrometheusExpression expr) {
        PrometheusVisitor.super.visit(expr);
    }

    @Override
    public void visit(PrometheusTableReference ref) {

    }

    @Override
    public void visit(PrometheusSchemaReference ref) {

    }

    @Override
    public void visit(PrometheusConstant constant) {
        constraintStack.add(ConstraintValueGenerator.genConstant(constant.getTextRepresentation()));
    }

    @Override
    public void visit(PrometheusColumnReference timeSeries) {
        // TODO 默认转换为列约束 -> 约束条件为全局
        constraintStack.add(ConstraintValueGenerator.genTimeSeries(timeSeries.getColumn().getName()));
    }

    @Override
    public void visit(UnaryOperation<PrometheusExpression> unaryOperation) {
        if (unaryOperation instanceof PrometheusUnaryNotPrefixOperation)
            visit((PrometheusUnaryNotPrefixOperation) unaryOperation);
        if (unaryOperation instanceof PrometheusUnaryPrefixOperation)
            visit((PrometheusUnaryPrefixOperation) unaryOperation);
    }

    @Override
    public void visit(PrometheusBinaryLogicalOperation op) {
        constraintStack.add(ConstraintValueGenerator.genOperator(op.getTextRepresentation()));
        visit(op.getLeft());
        visit(op.getRight());
        // 将结果存储, 弹出运算符
        ConstraintValue right = constraintStack.pop();
        ConstraintValue left = constraintStack.pop();
        ConstraintValue logicOp = constraintStack.pop();
        // 单列 -> 永 true 的列约束
        if (left instanceof ConstraintValue.TimeSeriesValue) {
            left = ConstraintValueGenerator.genTrueConstraint();
        }
        if (right instanceof ConstraintValue.TimeSeriesValue) {
            right = ConstraintValueGenerator.genTrueConstraint();
        }

        // 仅支持列约束和列约束进行运算
        if (!(left instanceof ConstraintValue.TimeSeriesConstraintValue
                && right instanceof ConstraintValue.TimeSeriesConstraintValue)) {
            log.error("仅支持列约束和列约束进行运算");
            throw new IgnoreMeException();
        }
        ConstraintValue baseTimeSeriesConstraintLeft = left.transToBaseSeq(databaseName, tableName);
        ConstraintValue baseTimeSeriesConstraintRight = right.transToBaseSeq(databaseName, tableName);

        // 全部转为baseTimeSeries
        if (PrometheusBinaryLogicalOperation.PrometheusBinaryLogicalOperator.AND ==
                PrometheusBinaryLogicalOperation.PrometheusBinaryLogicalOperator.getOperatorByText(logicOp.getOperator())) {
            TimeSeriesConstraint baseTimeSeriesConstraint = baseTimeSeriesConstraintLeft.getTimeSeriesConstraint()
                    .intersects(baseTimeSeriesConstraintRight.getTimeSeriesConstraint());
            constraintStack.add(ConstraintValueGenerator.genConstraint(baseTimeSeriesConstraintLeft.getTimeSeriesValue(),
                    baseTimeSeriesConstraint));
        } else {
            TimeSeriesConstraint baseTimeSeriesConstraint = baseTimeSeriesConstraintLeft.getTimeSeriesConstraint()
                    .union(baseTimeSeriesConstraintRight.getTimeSeriesConstraint(),
                            PrometheusConstant.createFloatArithmeticTolerance());
            baseTimeSeriesConstraint.setTimeSeriesName(GlobalConstant.BASE_TIME_SERIES_NAME);
            constraintStack.add(ConstraintValueGenerator.genConstraint(baseTimeSeriesConstraintLeft.getTimeSeriesValue(),
                    baseTimeSeriesConstraint));
        }
    }

    @Override
    public void visit(PrometheusSelect s) {

    }

    @Override
    public void visit(PrometheusBinaryComparisonOperation op) {
        // TODO PromQL 获取的是最终计算结果，而不是原始样本值 => 跳过计算中间结果的表达式
        // TODO 两个时序在相同时间戳上对值进行比较
        // TODO 桶计算方式解决该难点
        visit(op.getLeft());
        visit(op.getRight());
        ConstraintValue right = constraintStack.pop();
        ConstraintValue left = constraintStack.pop();

        if (left.isConstant() && right.isConstant()) {
            // 值 and 值 比较
            PrometheusConstant expectedValue = op.getExpectedValue();
            if (expectedValue.asBooleanNotNull())
                constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
            else constraintStack.add(ConstraintValueGenerator.genFalseConstraint());
            return;
        }

        ConstraintValue.TimeSeriesValue timeSeriesValue;
        BigDecimal constantValue;
        PrometheusBinaryComparisonOperation.BinaryComparisonOperator operator = op.getOp();
        // 格式均调整为 左边列 右边值
        if (left.isTimeSeries() && right.isConstant()) {
            timeSeriesValue = (ConstraintValue.TimeSeriesValue) left;
            constantValue = new BigDecimal(right.getConstant());
        } else if (left.isConstant() && right.isTimeSeries()) {
            timeSeriesValue = (ConstraintValue.TimeSeriesValue) right;
            operator = operator.reverseInequality();
            constantValue = new BigDecimal(left.getConstant());
        } else if (left.isTimeSeries() && right.isTimeSeries()) {
            // 列 and 列 比较 (列相同) -> 交换位置
            ConstraintValue.TimeSeriesValue leftTimeSeriesValue = (ConstraintValue.TimeSeriesValue)
                    left.transToBaseSeq(databaseName, tableName);
            ConstraintValue.TimeSeriesValue rightTimeSeriesValue = (ConstraintValue.TimeSeriesValue)
                    right.transToBaseSeq(databaseName, tableName);
            leftTimeSeriesValue.subFactor(rightTimeSeriesValue.getFactor());
            rightTimeSeriesValue.subIntercept(leftTimeSeriesValue.getIntercept());
            // 消除timeSeries
            timeSeriesValue = leftTimeSeriesValue;
            timeSeriesValue.setInterceptZero();
            constantValue = rightTimeSeriesValue.getIntercept().getBigDecimalConstant();
        } else {
            // TODO 在同一个时间戳上进行值比较
            // 不支持BOOL值比较
            // TODO 列 and 时序约束 比较
            // TODO (tsafdb_0_d5d4649c_f7f8_4b59_a917_23a04a8d0174 < 510) >= (tsafdb_0_d5d4649c_f7f8_4b59_a917_23a04a8d0174 < 512)
            // TODO (tsafdb_0_d5d4649c_f7f8_4b59_a917_23a04a8d0174 < 510) > bool (1 < bool (1))
            log.warn("列约束参与比较运算, Prometheus不支持BOOL值比较");
            throw new IgnoreMeException();
        }

        // 列系数为0值, 直接比较常量截距 (系数为0后)
        if (timeSeriesValue.getFactor().getBigDecimalConstant().compareTo(BigDecimal.ZERO) == 0) {
            if (operator.getExpectedValue(PrometheusConstant.createBigDecimalConstant(
                            timeSeriesValue.getIntercept().getBigDecimalConstant()),
                    PrometheusConstant.createBigDecimalConstant(constantValue)).asBooleanNotNull())
                constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
            else constraintStack.add(ConstraintValueGenerator.genFalseConstraint());
            return;
        }
        // TODO 非常量列因子待实现
        // 消除列因子: 转换常量值, 比较运算符方向
        if (timeSeriesValue.hasNegativeFactor()) operator = operator.reverseInequality();
        constantValue = timeSeriesValue.eliminateFactor(constantValue);

        String timeSeriesName = timeSeriesValue.getTimeSeriesName();
        TimeSeriesConstraint curConstraint;
        switch (operator) {
            case EQUALS:
                curConstraint = new TimeSeriesConstraint(timeSeriesName, new RangeConstraint(constantValue, constantValue));
                break;
            case NOT_EQUALS:
                curConstraint = new TimeSeriesConstraint(timeSeriesName);
                curConstraint.addNotEqualValue(constantValue);
                break;
            case LESS:
                curConstraint = new TimeSeriesConstraint(timeSeriesName, RangeConstraint.builder()
                        .greaterEqualValue(new BigDecimal(Long.MIN_VALUE))
                        .lessEqualValue(constantValue).build());
                curConstraint.addNotEqualValue(constantValue);
                break;
            case LESS_EQUALS:
                curConstraint = new TimeSeriesConstraint(timeSeriesName, RangeConstraint.builder()
                        .greaterEqualValue(new BigDecimal(Long.MIN_VALUE))
                        .lessEqualValue(constantValue).build());
                break;
            case GREATER:
                curConstraint = new TimeSeriesConstraint(timeSeriesName, RangeConstraint.builder()
                        .greaterEqualValue(constantValue)
                        .lessEqualValue(new BigDecimal(Long.MAX_VALUE)).build());
                curConstraint.addNotEqualValue(constantValue);
                break;
            case GREATER_EQUALS:
                curConstraint = new TimeSeriesConstraint(timeSeriesName, RangeConstraint.builder()
                        .greaterEqualValue(constantValue)
                        .lessEqualValue(new BigDecimal(Long.MAX_VALUE)).build());
                break;
            default:
                throw new AssertionError();
        }
        // 生成单个约束条件
        constraintStack.add(ConstraintValueGenerator.genConstraint(timeSeriesValue, curConstraint));
    }

//    @Override
//    public void visit(PrometheusCastOperation op) {
//        visit(op.getExpr());
//        ConstraintValue exprValue = constraintStack.pop();
//        // 其他类型强转均满足 不损失精度的转换, 故TSQS中强转不会对Y轴增加额外的约束条件
//        if (exprValue.isTimeSeries() && op.getCastType().equalsIgnoreCase(PrometheusCastOperation.CastType.BOOL.getType())) {
//            // 列强转BOOL值 -> 约束列不为0值(Prometheus 非0值转为TRUE)
//            TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint(exprValue.getTimeSeriesName());
//            timeSeriesConstraint.addNotEqualValue(BigDecimal.ZERO);
//            constraintStack.add(ConstraintValueGenerator.genConstraint(exprValue.getTimeSeriesValue(),
//                    timeSeriesConstraint));
//        } else constraintStack.add(exprValue);
//    }

//    @Override
//    public void visit(PrometheusBinaryOperation op) {
//        // TODO
//        log.warn("PrometheusBinaryOperation暂不支持按位运算");
//        throw new IgnoreMeException();
//    }

    @Override
    public void visit(PrometheusUnaryPrefixOperation op) {
        visit(op.getExpression());
        ConstraintValue constraintValue = constraintStack.pop();
        // 约束条件取反
        if (op.getOp() == PrometheusUnaryPrefixOperation.PrometheusUnaryPrefixOperator.MINUS)
            constraintStack.add(constraintValue.negate());
        else
            constraintStack.add(constraintValue);
    }

    @Override
    public void visit(PrometheusUnaryNotPrefixOperation op) {
        // Unary NOT Prefix Operation
        visit(op.getExpression());
        ConstraintValue constraintValue = constraintStack.pop();
        if (constraintValue.isTimeSeries()) {
            // 列表达式系数被减法清除 => NOT 常量 => TRUE
            ConstraintValue.TimeSeriesValue timeSeriesValue = (ConstraintValue.TimeSeriesValue) constraintValue;
            if (timeSeriesValue.getFactor().getBigDecimalConstant().compareTo(BigDecimal.ZERO) == 0) {
                if (op.getExpression().getExpectedValue().castAs(PrometheusSchema.CommonDataType.BIGDECIMAL)
                        .getBigDecimalValue().compareTo(timeSeriesValue.getIntercept().getBigDecimalConstant()) == 0)
                    constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
                else constraintStack.add(ConstraintValueGenerator.genFalseConstraint());
                return;
            }

            // 列预期值, (NOT timeSeries) 即将其预期值取TRUE
            BigDecimal expectedValue = op.getExpression().getExpectedValue().castAs(
                    PrometheusSchema.CommonDataType.BIGDECIMAL).getBigDecimalValue();
            // 消除列因子
            expectedValue = timeSeriesValue.eliminateFactor(expectedValue);
            BigDecimal lessValue = expectedValue;
            BigDecimal greaterValue = expectedValue;
            // 浮点运算存在精度损失
            if (BigDecimalUtil.isDouble(expectedValue)) {
                greaterValue = expectedValue.subtract(
                        BigDecimal.valueOf(Math.pow(10, -PrometheusConstant.PrometheusDoubleConstant.scale)));
                lessValue = expectedValue.add(
                        BigDecimal.valueOf(Math.pow(10, -PrometheusConstant.PrometheusDoubleConstant.scale)));
            }
            TimeSeriesConstraint curConstraint = new TimeSeriesConstraint(timeSeriesValue.getTimeSeriesName(), new RangeConstraint(
                    greaterValue, lessValue));
            constraintStack.add(ConstraintValueGenerator.genConstraint(timeSeriesValue, curConstraint));
        } else if (constraintValue.isConstant()) {
            // TRUE -> 常量覆盖区间所有范围
            constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
        } else if (constraintValue.isTimeSeriesConstraint()) {
            // 列约束取反
            constraintStack.add(constraintValue.negate());
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public void visit(PrometheusBinaryArithmeticOperation op) {
        visit(op.getLeft());
        visit(op.getRight());
        ConstraintValue right = constraintStack.pop();
        ConstraintValue left = constraintStack.pop();

        PrometheusBinaryArithmeticOperation.PrometheusBinaryArithmeticOperator operator = op.getOp();
        // 列 op 列
        if (left.isTimeSeries() && right.isTimeSeries()) {
            // 转换为相同的基序列
            ConstraintValue.TimeSeriesValue timeSeriesValueLeft = (ConstraintValue.TimeSeriesValue)
                    left.transToBaseSeq(databaseName, tableName);
            ConstraintValue.TimeSeriesValue timeSeriesValueRight = (ConstraintValue.TimeSeriesValue)
                    right.transToBaseSeq(databaseName, tableName);
            // 因子系数和截距相加减
            switch (operator) {
                case PLUS:
                    timeSeriesValueLeft.addFactor(timeSeriesValueRight.getFactor());
                    timeSeriesValueLeft.addIntercept(timeSeriesValueRight.getIntercept());
                    break;
                case SUBTRACT:
                    timeSeriesValueLeft.subFactor(timeSeriesValueRight.getFactor());
                    timeSeriesValueLeft.subIntercept(timeSeriesValueRight.getIntercept());
                    break;
                case MULTIPLY:
                case DIVIDE:
                case MODULO:
                    // TODO 暂不支持列和列进行乘除操作
                    log.warn("暂不支持列和列进行乘、除、求模操作.");
                    throw new IgnoreMeException();
                default:
                    throw new AssertionError();
            }
            constraintStack.add(timeSeriesValueLeft);
            return;
        }

        // 常量 op 常量
        if (left.isConstant() && right.isConstant()) {
            visit(op.getExpectedValue());
            return;
        }

        // 列 op 常量 / 常量 op 列
        ConstraintValue.TimeSeriesValue timeSeriesValue;
        ConstraintValue.ConstantValue constantValue;
        if (left.isConstant() && right.isTimeSeries()) {
            timeSeriesValue = (ConstraintValue.TimeSeriesValue) right;
            constantValue = (ConstraintValue.ConstantValue) left;
            // 常量 / 列
            if (operator == PrometheusBinaryArithmeticOperation.PrometheusBinaryArithmeticOperator.DIVIDE) {
                timeSeriesValue.transToReciprocalVal(constantValue);
                constraintStack.add(timeSeriesValue);
                return;
            }
            // 针对列的减法 => 列约束条件取反, 变为加法
            if (operator == PrometheusBinaryArithmeticOperation.PrometheusBinaryArithmeticOperator.SUBTRACT) {
                timeSeriesValue = timeSeriesValue.negate();
                operator = PrometheusBinaryArithmeticOperation.PrometheusBinaryArithmeticOperator.PLUS;
            }
        } else if (left.isTimeSeries() && right.isConstant()) {
            timeSeriesValue = (ConstraintValue.TimeSeriesValue) left;
            constantValue = (ConstraintValue.ConstantValue) right;
        } else {
            log.error("约束op约束, left:{} right:{}", left, right);
            throw new IgnoreMeException();
        }

        // 将常量作为倍率因子附加于first中
        switch (operator) {
            case PLUS:
                timeSeriesValue.addIntercept(constantValue);
                break;
            case SUBTRACT:
                timeSeriesValue.subIntercept(constantValue);
                break;
            case MULTIPLY:
                timeSeriesValue.multiplyFactor(constantValue);
                break;
            case DIVIDE:
                constantValue = ConstraintValueGenerator.genConstant(new BigDecimal(1)
                        .divide(constantValue.getBigDecimalConstant(), DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP)
                        .toPlainString());
                timeSeriesValue.multiplyFactor(constantValue);
                break;
            case MODULO:
                log.warn("不支持MODULO");
                throw new IgnoreMeException();
            default:
                throw new AssertionError();
        }
        constraintStack.add(timeSeriesValue);
    }

//    @Override
//    public void visit(PrometheusOrderByTerm op) {
//        // TODO
//    }

//    @Override
//    public void visit(PrometheusUnaryPostfixOperation op) {
//        visit(op.getExpression());
//        ConstraintValue exprValue = constraintStack.pop();
//
//        if (exprValue.isConstant() && !op.isNegated())
//            constraintStack.add(ConstraintValueGenerator.genFalseConstraint());
//        else if (exprValue.isConstant() && op.isNegated())
//            constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
//        else if (exprValue.isTimeSeries() && !op.isNegated()) {
//            // timeSeriesValue is null -> timestamp list
//            constraintStack.add(ConstraintValueGenerator.genConstraint(
//                    ConstraintValueGenerator.genTimeSeries(PrometheusConstantString.TIME_FIELD_NAME.getName()),
//                    nullValueTimestamps));
//        } else if (exprValue.isTimeSeries() && op.isNegated()) {
//            // 将timeSeriesConstraint因子截距设为0
//            TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint(exprValue.getTimeSeriesName(),
//                    new RangeConstraint());
//            constraintStack.add(ConstraintValueGenerator.genConstraint(exprValue.getTimeSeriesValue(),
//                    timeSeriesConstraint));
//        } else if (exprValue.isTimeSeriesConstraint() && !op.isNegated()) {
//            TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint(GlobalConstant.BASE_TIME_SERIES_NAME,
//                    new RangeConstraint());
//            timeSeriesConstraint.getRangeConstraints().clear();
//            constraintStack.add(ConstraintValueGenerator.genConstraint(ConstraintValueGenerator.genBaseTimeSeries(),
//                    timeSeriesConstraint));
//        } else if (exprValue.isTimeSeriesConstraint() && op.isNegated()) {
//            constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
//        } else {
//            throw new AssertionError();
//        }
//    }
//
//    @Override
//    public void visit(PrometheusInOperation op) {
//        if (op.getListElements().isEmpty()) throw new AssertionError();
//
//        // 将IN转换为OR表达式组合
//        PrometheusBinaryComparisonOperation.BinaryComparisonOperator comparisonOperator;
//        PrometheusBinaryLogicalOperation.PrometheusBinaryLogicalOperator logicalOperator;
//        if (!op.isTrue()) {
//            comparisonOperator = PrometheusBinaryComparisonOperation.BinaryComparisonOperator.NOT_EQUALS;
//            logicalOperator = PrometheusBinaryLogicalOperation.PrometheusBinaryLogicalOperator.AND;
//        } else {
//            comparisonOperator = PrometheusBinaryComparisonOperation.BinaryComparisonOperator.EQUALS;
//            logicalOperator = PrometheusBinaryLogicalOperation.PrometheusBinaryLogicalOperator.OR;
//        }
//
//        int index = 0;
//        PrometheusExpression combinationExpr = new PrometheusBinaryComparisonOperation(op.getExpr(),
//                op.getListElements().get(index++), comparisonOperator);
//
//        for (; index < op.getListElements().size(); index++) {
//            PrometheusBinaryComparisonOperation right = new PrometheusBinaryComparisonOperation(op.getExpr(),
//                    op.getListElements().get(index), comparisonOperator);
//            combinationExpr = new PrometheusBinaryLogicalOperation(combinationExpr, right, logicalOperator);
//        }
//        visit(combinationExpr);
//    }
//
//    @Override
//    public void visit(PrometheusBetweenOperation op) {
//        // 将BETWEEN转换为LESS_EQUALS
//        PrometheusBinaryComparisonOperation left = new PrometheusBinaryComparisonOperation(op.getLeft(), op.getExpr(),
//                PrometheusBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
//        PrometheusBinaryComparisonOperation right = new PrometheusBinaryComparisonOperation(op.getExpr(), op.getRight(),
//                PrometheusBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
//        visit(new PrometheusBinaryLogicalOperation(left, right,
//                PrometheusBinaryLogicalOperation.PrometheusBinaryLogicalOperator.AND));
//    }
//
//    @Override
//    public void visit(PrometheusComputableFunction f) {
//        visit(f.getExpectedValue());
//    }
}
