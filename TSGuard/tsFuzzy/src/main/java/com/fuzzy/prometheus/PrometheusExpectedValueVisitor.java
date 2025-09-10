package com.fuzzy.prometheus;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.visitor.UnaryOperation.OperatorKind;
import com.fuzzy.prometheus.ast.*;

public class PrometheusExpectedValueVisitor implements PrometheusVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(PrometheusExpression expr) {
        PrometheusToStringVisitor v = new PrometheusToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < nrTabs; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    @Override
    public void visit(PrometheusTableReference ref) {
        print(ref);
    }

    @Override
    public void visit(PrometheusSchemaReference ref) {
        print(ref);
    }

    @Override
    public void visit(PrometheusConstant constant) {
        print(constant);
    }

    @Override
    public void visit(PrometheusColumnReference column) {
        print(column);
    }

    @Override
    public void visit(PrometheusBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(PrometheusSelect select) {
        for (PrometheusExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(PrometheusBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

//    @Override
//    public void visit(PrometheusCastOperation op) {
//        print(op);
//        visit(op.getExpr());
//    }

    @Override
    public void visit(PrometheusBinaryArithmeticOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

//    @Override
//    public void visit(PrometheusBinaryOperation op) {
//        print(op);
//        visit(op.getLeft());
//        visit(op.getRight());
//    }

    @Override
    public void visit(PrometheusUnaryPrefixOperation op) {
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        if (op.getOperatorKind() == OperatorKind.PREFIX) {
            sb.append(op.getOperatorRepresentation());
            sb.append(' ');
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        visit(op.getExpression());
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
        if (op.getOperatorKind() == OperatorKind.POSTFIX) {
            sb.append(' ');
            sb.append(op.getOperatorRepresentation());
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
    }

    @Override
    public void visit(PrometheusUnaryNotPrefixOperation op) {
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        if (op.getOperatorKind() == OperatorKind.PREFIX) {
            sb.append(op.getOperatorRepresentation());
            sb.append(' ');
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        visit(op.getExpression());
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
        if (op.getOperatorKind() == OperatorKind.POSTFIX) {
            sb.append(' ');
            sb.append(op.getOperatorRepresentation());
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
    }

//    @Override
//    public void visit(PrometheusOrderByTerm op) {
//
//    }
//
//    @Override
//    public void visit(PrometheusUnaryPostfixOperation op) {
//        print(op);
//        visit(op.getExpression());
//    }
//
//    @Override
//    public void visit(PrometheusInOperation op) {
//        print(op);
//        for (PrometheusExpression right : op.getListElements())
//            visit(right);
//    }
//
//    @Override
//    public void visit(PrometheusBetweenOperation op) {
//        print(op);
//        visit(op.getExpr());
//        visit(op.getLeft());
//        visit(op.getRight());
//    }

//    @Override
//    public void visit(PrometheusExists op) {
//        print(op);
//        visit(op.getExpr());
//    }
//
//    @Override
//    public void visit(PrometheusStringExpression op) {
//        print(op);
//    }

//    @Override
//    public void visit(PrometheusComputableFunction f) {
//        print(f);
//        for (PrometheusExpression expr : f.getArguments()) {
//            visit(expr);
//        }
//    }

    @Override
    public void visit(PrometheusExpression expr) {
        nrTabs++;
        try {
            PrometheusVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    public String get() {
        return sb.toString();
    }
}
