package com.fuzzy.prometheus;

import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.visitor.ToStringVisitor;
import com.fuzzy.common.visitor.UnaryOperation;
import com.fuzzy.prometheus.ast.*;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class PrometheusToStringVisitor extends ToStringVisitor<PrometheusExpression> implements PrometheusVisitor {

    // 抽象语法树转 PromQL 表达式
    int ref;
    // 是否抽象语法节点
    boolean isAbstractExpression;

    public PrometheusToStringVisitor() {
        isAbstractExpression = false;
    }

    public PrometheusToStringVisitor(boolean isAbstractExpression) {
        this.isAbstractExpression = isAbstractExpression;
    }

    @Override
    public void visitSpecific(PrometheusExpression expr) {
        PrometheusVisitor.super.visit(expr);
    }

    @Override
    public void visit(PrometheusTableReference ref) {
        if (!isAbstractExpression) {
            // TODO
            sb.append(ref.getTable().getName());
        } else sb.append(GlobalConstant.TABLE_NAME);
    }

    @Override
    public void visit(PrometheusSchemaReference ref) {
        if (!isAbstractExpression) sb.append(ref.getSchema().getRandomTable().getDatabaseName());
        else sb.append(GlobalConstant.DATABASE_NAME);
    }

    @Override
    public void visit(PrometheusConstant constant) {
        if (!isAbstractExpression) sb.append(constant.getTextRepresentation());
        else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(PrometheusColumnReference column) {
        String databaseName = column.getColumn().getTable().getDatabaseName();
        String tableName = column.getColumn().getTable().getName();
        String columnName = column.getColumn().getName();
        String timeSeriesName = String.format("%s{table=\"%s\", timeSeries=\"%s\"}", databaseName, tableName, columnName);
        // TODO
//        if (isAbstractExpression && !columnName.equalsIgnoreCase(PrometheusConstantString.TIME_FIELD_NAME.getName()))
//            columnName = GlobalConstant.COLUMN_NAME;
        sb.append(timeSeriesName);
    }

    public void visitDoubleValueLeft(PrometheusConstant constant) {
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .subtract(BigDecimal.valueOf(Math.pow(10, -PrometheusConstant.PrometheusDoubleConstant.scale)))
                    .setScale(PrometheusConstant.PrometheusDoubleConstant.scale, RoundingMode.HALF_UP)
                    .toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
        sb.append(" <= ");
    }

    public void visitDoubleValueRight(PrometheusConstant constant) {
        sb.append(" <= ");
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .add(BigDecimal.valueOf(Math.pow(10, -PrometheusConstant.PrometheusDoubleConstant.scale)))
                    .setScale(PrometheusConstant.PrometheusDoubleConstant.scale, RoundingMode.HALF_UP)
                    .toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(PrometheusBinaryLogicalOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(")");

        sb.append(" ");
        sb.append(op.getTextRepresentation());
        sb.append(" ");

        sb.append("(");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(PrometheusSelect s) {
        if (s.getWhereClause() != null) {
            PrometheusExpression whereClause = s.getWhereClause();
            visit(whereClause);
        }
    }

    @Override
    public void visit(PrometheusBinaryComparisonOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());

        // comparisons between scalars must use BOOL modifier
        if (op.getLeft().isScalarExpression() && op.getRight().isScalarExpression()) sb.append(" bool");

        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

//    @Override
//    public void visit(PrometheusCastOperation op) {
//        sb.append(" CAST(");
//        visit(op.getExpr());
//        sb.append(" as ");
//        sb.append(op.getCastType());
//        sb.append(")");
//    }

//    @Override
//    public void visit(PrometheusBinaryOperation op) {
//        sb.append("(");
//        visit(op.getLeft());
//        sb.append(") ");
//        sb.append(op.getOp().getTextRepresentation());
//        sb.append(" (");
//        visit(op.getRight());
//        sb.append(")");
//    }

    @Override
    public void visit(PrometheusUnaryPrefixOperation op) {
        super.visit((UnaryOperation<PrometheusExpression>) op);
    }

    @Override
    public void visit(PrometheusUnaryNotPrefixOperation unaryOperation) {
        // Unary NOT Prefix Operation
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
        PrometheusUnaryNotPrefixOperation.PrometheusUnaryNotPrefixOperator op =
                ((PrometheusUnaryNotPrefixOperation) unaryOperation).getOp();
        // NOT DOUBLE
        if (op.equals(PrometheusUnaryNotPrefixOperation.PrometheusUnaryNotPrefixOperator.NOT_DOUBLE)) {
            visitDoubleValueLeft(unaryOperation.getExpression().getExpectedValue());
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
            visit(unaryOperation.getExpression());
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
            sb.append(" AND ");
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
            visit(unaryOperation.getExpression());
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
            visitDoubleValueRight(unaryOperation.getExpression().getExpectedValue());
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
            return;
        }

        // NOT INT
        visit(unaryOperation.getExpression().getExpectedValue());
        // 子节点为常量、列、强转运算时, NOT符号改为 =
        if (unaryOperation.getExpression() instanceof PrometheusConstant
                || unaryOperation.getExpression() instanceof PrometheusColumnReference
                || unaryOperation.getExpression() instanceof PrometheusBinaryLogicalOperation
                || unaryOperation.getExpression() instanceof PrometheusUnaryPrefixOperation
                || unaryOperation.getExpression() instanceof PrometheusBinaryArithmeticOperation
//                || unaryOperation.getExpression() instanceof PrometheusBinaryOperation
//                || unaryOperation.getExpression() instanceof PrometheusComputableFunction
        )
            sb.append(" == ");

        if (unaryOperation.getExpression() instanceof PrometheusConstant) {
            sb.append("bool ");
        }
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
        visit(unaryOperation.getExpression());
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
    }

    @Override
    public void visit(PrometheusBinaryArithmeticOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

//    @Override
//    public void visit(PrometheusOrderByTerm op) {
//        visit(op.getExpr());
//        sb.append(" ");
//        sb.append(op.getOrder() == PrometheusOrder.ASC ? "ASC" : "DESC");
//    }

//    @Override
//    public void visit(PrometheusUnaryPostfixOperation op) {
//        sb.append("(");
//        visit(op.getExpression());
//        sb.append(")");
//        sb.append(" IS ");
//        if (op.isNegated()) {
//            sb.append("NOT ");
//        }
//        switch (op.getOperator()) {
//            case IS_NULL:
//                sb.append("NULL");
//                break;
//            default:
//                throw new AssertionError(op);
//        }
//    }

//    @Override
//    public void visit(PrometheusExists op) {
//        // TODO
//        sb.append(" EXISTS (");
//        visit(op.getExpr());
//        sb.append(")");
//    }

//    @Override
//    public void visit(PrometheusStringExpression op) {
//        sb.append(op.getStr());
//    }

//    @Override
//    public void visit(PrometheusInOperation op) {
//        sb.append("(");
//        visit(op.getExpr());
//        sb.append(")");
//        if (!op.isTrue()) sb.append(" NOT");
//        sb.append(" IN ");
//        sb.append("(");
//        for (int i = 0; i < op.getListElements().size(); i++) {
//            if (i != 0) sb.append(", ");
//            visit(op.getListElements().get(i));
//        }
//        sb.append(")");
//    }

//    @Override
//    public void visit(PrometheusBetweenOperation op) {
//        sb.append("(");
//        visit(op.getExpr());
//        sb.append(") BETWEEN (");
//        visit(op.getLeft());
//        sb.append(") AND (");
//        visit(op.getRight());
//        sb.append(")");
//    }

    @Override
    public void visit(UnaryOperation<PrometheusExpression> unaryOperation) {
        if (unaryOperation instanceof PrometheusUnaryNotPrefixOperation)
            visit((PrometheusUnaryNotPrefixOperation) unaryOperation);
        else if (unaryOperation instanceof PrometheusUnaryPrefixOperation)
            visit((PrometheusUnaryPrefixOperation) unaryOperation);
    }

//    @Override
//    public void visit(PrometheusComputableFunction f) {
//        sb.append(f.getFunction().getName());
//        sb.append("(");
//        for (int i = 0; i < f.getArguments().length; i++) {
//            if (i != 0) sb.append(", ");
//            visit(f.getArguments()[i]);
//        }
//        sb.append(")");
//    }

}
