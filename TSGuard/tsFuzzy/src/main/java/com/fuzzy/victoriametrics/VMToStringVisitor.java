package com.fuzzy.victoriametrics;

import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.common.visitor.ToStringVisitor;
import com.fuzzy.common.visitor.UnaryOperation;
import com.fuzzy.victoriametrics.ast.*;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class VMToStringVisitor extends ToStringVisitor<VMExpression> implements VMVisitor {

    // 抽象语法树转 PromQL 表达式
    int ref;
    // 是否抽象语法节点
    boolean isAbstractExpression;

    public VMToStringVisitor() {
        isAbstractExpression = false;
    }

    public VMToStringVisitor(boolean isAbstractExpression) {
        this.isAbstractExpression = isAbstractExpression;
    }

    @Override
    public void visitSpecific(VMExpression expr) {
        VMVisitor.super.visit(expr);
    }

    @Override
    public void visit(VMTableReference ref) {
        if (!isAbstractExpression) {
            // TODO
            sb.append(ref.getTable().getName());
        } else sb.append(GlobalConstant.TABLE_NAME);
    }

    @Override
    public void visit(VMSchemaReference ref) {
        if (!isAbstractExpression) {
            sb.append(ref.getSchema().getRandomTable().getDatabaseName())
                    .append(TimeSeriesLabelConstant.END_WITH_VALUE.getLabel());
        } else sb.append(GlobalConstant.DATABASE_NAME);
    }

    @Override
    public void visit(VMConstant constant) {
        if (!isAbstractExpression) sb.append(constant.getTextRepresentation());
        else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(VMColumnReference column) {
        String databaseName = column.getColumn().getTable().getDatabaseName();
        String tableName = column.getColumn().getTable().getName();
        String columnName = column.getColumn().getName();
        String timeSeriesName = String.format("%s{table=\"%s\", timeSeries=\"%s\"}",
                databaseName + TimeSeriesLabelConstant.END_WITH_VALUE.getLabel(), tableName, columnName);
        // TODO
//        if (isAbstractExpression && !columnName.equalsIgnoreCase(VMConstantString.TIME_FIELD_NAME.getName()))
//            columnName = GlobalConstant.COLUMN_NAME;
        sb.append(timeSeriesName);
    }

    public void visitDoubleValueLeft(VMConstant constant) {
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .subtract(BigDecimal.valueOf(Math.pow(10, -VMConstant.VMDoubleConstant.scale)))
                    .setScale(VMConstant.VMDoubleConstant.scale, RoundingMode.HALF_UP)
                    .toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
        sb.append(" <= ");
    }

    public void visitDoubleValueRight(VMConstant constant) {
        sb.append(" <= ");
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .add(BigDecimal.valueOf(Math.pow(10, -VMConstant.VMDoubleConstant.scale)))
                    .setScale(VMConstant.VMDoubleConstant.scale, RoundingMode.HALF_UP)
                    .toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(VMBinaryLogicalOperation op) {
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
    public void visit(VMSelect s) {
        if (s.getWhereClause() != null) {
            VMExpression whereClause = s.getWhereClause();
            visit(whereClause);
        }
    }

    @Override
    public void visit(VMBinaryComparisonOperation op) {
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
//    public void visit(VMCastOperation op) {
//        sb.append(" CAST(");
//        visit(op.getExpr());
//        sb.append(" as ");
//        sb.append(op.getCastType());
//        sb.append(")");
//    }

//    @Override
//    public void visit(VMBinaryOperation op) {
//        sb.append("(");
//        visit(op.getLeft());
//        sb.append(") ");
//        sb.append(op.getOp().getTextRepresentation());
//        sb.append(" (");
//        visit(op.getRight());
//        sb.append(")");
//    }

    @Override
    public void visit(VMUnaryPrefixOperation op) {
        super.visit((UnaryOperation<VMExpression>) op);
    }

//    @Override
//    public void visit(VMUnaryNotPrefixOperation unaryOperation) {
//        // Unary NOT Prefix Operation
//        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
//        VMUnaryNotPrefixOperation.VMUnaryNotPrefixOperator op =
//                ((VMUnaryNotPrefixOperation) unaryOperation).getOp();
//        // NOT DOUBLE
//        if (op.equals(VMUnaryNotPrefixOperation.VMUnaryNotPrefixOperator.NOT_DOUBLE)) {
//            visitDoubleValueLeft(unaryOperation.getExpression().getExpectedValue());
//            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
//            visit(unaryOperation.getExpression());
//            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
//            sb.append(" AND ");
//            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
//            visit(unaryOperation.getExpression());
//            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
//            visitDoubleValueRight(unaryOperation.getExpression().getExpectedValue());
//            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
//            return;
//        }
//
//        // NOT INT
//        visit(unaryOperation.getExpression().getExpectedValue());
//        // 子节点为常量、列、强转运算时, NOT符号改为 =
//        if (unaryOperation.getExpression() instanceof VMConstant
//                || unaryOperation.getExpression() instanceof VMColumnReference
//                || unaryOperation.getExpression() instanceof VMBinaryLogicalOperation
//                || unaryOperation.getExpression() instanceof VMUnaryPrefixOperation
//                || unaryOperation.getExpression() instanceof VMBinaryArithmeticOperation
////                || unaryOperation.getExpression() instanceof VMBinaryOperation
////                || unaryOperation.getExpression() instanceof VMComputableFunction
//        )
//            sb.append(" == ");
//
//        if (unaryOperation.getExpression() instanceof VMConstant) {
//            sb.append("bool ");
//        }
//        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
//        visit(unaryOperation.getExpression());
//        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
//        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
//    }

    @Override
    public void visit(VMBinaryArithmeticOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

//    @Override
//    public void visit(VMOrderByTerm op) {
//        visit(op.getExpr());
//        sb.append(" ");
//        sb.append(op.getOrder() == VMOrder.ASC ? "ASC" : "DESC");
//    }

//    @Override
//    public void visit(VMUnaryPostfixOperation op) {
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
//    public void visit(VMExists op) {
//        // TODO
//        sb.append(" EXISTS (");
//        visit(op.getExpr());
//        sb.append(")");
//    }

//    @Override
//    public void visit(VMStringExpression op) {
//        sb.append(op.getStr());
//    }

//    @Override
//    public void visit(VMInOperation op) {
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
//    public void visit(VMBetweenOperation op) {
//        sb.append("(");
//        visit(op.getExpr());
//        sb.append(") BETWEEN (");
//        visit(op.getLeft());
//        sb.append(") AND (");
//        visit(op.getRight());
//        sb.append(")");
//    }

    @Override
    public void visit(UnaryOperation<VMExpression> unaryOperation) {
        if (unaryOperation instanceof VMUnaryPrefixOperation)
            visit((VMUnaryPrefixOperation) unaryOperation);
    }

//    @Override
//    public void visit(VMComputableFunction f) {
//        sb.append(f.getFunction().getName());
//        sb.append("(");
//        for (int i = 0; i < f.getArguments().length; i++) {
//            if (i != 0) sb.append(", ");
//            visit(f.getArguments()[i]);
//        }
//        sb.append(")");
//    }

}
