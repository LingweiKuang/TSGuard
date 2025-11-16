package com.fuzzy.victoriametrics;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.visitor.UnaryOperation.OperatorKind;
import com.fuzzy.victoriametrics.ast.*;

public class VMExpectedValueVisitor implements VMVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(VMExpression expr) {
        VMToStringVisitor v = new VMToStringVisitor();
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
    public void visit(VMTableReference ref) {
        print(ref);
    }

    @Override
    public void visit(VMSchemaReference ref) {
        print(ref);
    }

    @Override
    public void visit(VMConstant constant) {
        print(constant);
    }

    @Override
    public void visit(VMColumnReference column) {
        print(column);
    }

    @Override
    public void visit(VMBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(VMSelect select) {
        for (VMExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(VMBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

//    @Override
//    public void visit(VMCastOperation op) {
//        print(op);
//        visit(op.getExpr());
//    }

    @Override
    public void visit(VMBinaryArithmeticOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

//    @Override
//    public void visit(VMBinaryOperation op) {
//        print(op);
//        visit(op.getLeft());
//        visit(op.getRight());
//    }

    @Override
    public void visit(VMUnaryPrefixOperation op) {
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
//    public void visit(VMUnaryNotPrefixOperation op) {
//        if (!op.omitBracketsWhenPrinting()) {
//            sb.append('(');
//        }
//        if (op.getOperatorKind() == OperatorKind.PREFIX) {
//            sb.append(op.getOperatorRepresentation());
//            sb.append(' ');
//        }
//        if (!op.omitBracketsWhenPrinting()) {
//            sb.append('(');
//        }
//        visit(op.getExpression());
//        if (!op.omitBracketsWhenPrinting()) {
//            sb.append(')');
//        }
//        if (op.getOperatorKind() == OperatorKind.POSTFIX) {
//            sb.append(' ');
//            sb.append(op.getOperatorRepresentation());
//        }
//        if (!op.omitBracketsWhenPrinting()) {
//            sb.append(')');
//        }
//    }

//    @Override
//    public void visit(VMOrderByTerm op) {
//
//    }
//
//    @Override
//    public void visit(VMUnaryPostfixOperation op) {
//        print(op);
//        visit(op.getExpression());
//    }
//
//    @Override
//    public void visit(VMInOperation op) {
//        print(op);
//        for (VMExpression right : op.getListElements())
//            visit(right);
//    }
//
//    @Override
//    public void visit(VMBetweenOperation op) {
//        print(op);
//        visit(op.getExpr());
//        visit(op.getLeft());
//        visit(op.getRight());
//    }

//    @Override
//    public void visit(VMExists op) {
//        print(op);
//        visit(op.getExpr());
//    }
//
//    @Override
//    public void visit(VMStringExpression op) {
//        print(op);
//    }

//    @Override
//    public void visit(VMComputableFunction f) {
//        print(f);
//        for (VMExpression expr : f.getArguments()) {
//            visit(expr);
//        }
//    }

    @Override
    public void visit(VMExpression expr) {
        nrTabs++;
        try {
            VMVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    public String get() {
        return sb.toString();
    }
}
