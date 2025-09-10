package com.fuzzy.prometheus.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.ast.BinaryOperatorNode.Operator;
import com.fuzzy.common.ast.UnaryOperatorNode;

public class PrometheusUnaryNotPrefixOperation extends UnaryOperatorNode<PrometheusExpression,
        PrometheusUnaryNotPrefixOperation.PrometheusUnaryNotPrefixOperator> implements PrometheusExpression {

    public enum PrometheusUnaryNotPrefixOperator implements Operator {
        NOT_DOUBLE(" == ") {
            @Override
            public PrometheusConstant applyNotNull(PrometheusConstant expr) {
                return PrometheusConstant.createBoolean(!expr.asBooleanNotNull());
            }
        },
        NOT_INT(" == ") {
            @Override
            public PrometheusConstant applyNotNull(PrometheusConstant expr) {
                return PrometheusConstant.createBoolean(!expr.asBooleanNotNull());
            }
        };

        private String[] textRepresentations;

        PrometheusUnaryNotPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract PrometheusConstant applyNotNull(PrometheusConstant expr);

        public static PrometheusUnaryNotPrefixOperator getRandom(PrometheusExpression subExpr) {
            PrometheusExpression exprType = getNotPrefixTypeExpression(subExpr);

            PrometheusUnaryNotPrefixOperator operator;
            if (exprType instanceof PrometheusConstant && ((PrometheusConstant) exprType).isBoolean())
                throw new IllegalArgumentException();
            else if (exprType instanceof PrometheusConstant && ((PrometheusConstant) exprType).isDouble())
                operator = PrometheusUnaryNotPrefixOperator.NOT_DOUBLE;
            else operator = PrometheusUnaryNotPrefixOperator.NOT_INT;
            return operator;
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public PrometheusUnaryNotPrefixOperation(PrometheusExpression expr, PrometheusUnaryNotPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public void checkSyntax() {

    }

    @Override
    public PrometheusConstant getExpectedValue() {
        PrometheusConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return PrometheusConstant.createTrue();
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

    public static PrometheusUnaryNotPrefixOperation getNotUnaryPrefixOperation(PrometheusExpression expr) {
        PrometheusExpression exprType = getNotPrefixTypeExpression(expr);

        if (exprType instanceof PrometheusConstant && ((PrometheusConstant) exprType).isBoolean())
            throw new IllegalArgumentException();
        else if (exprType instanceof PrometheusConstant && ((PrometheusConstant) exprType).isDouble())
            return new PrometheusUnaryNotPrefixOperation(expr, PrometheusUnaryNotPrefixOperator.NOT_DOUBLE);
        else
            return new PrometheusUnaryNotPrefixOperation(expr, PrometheusUnaryNotPrefixOperator.NOT_INT);
    }

    private static PrometheusExpression getNotPrefixTypeExpression(PrometheusExpression expression) {
        PrometheusExpression exprType;
        // 一元操作符Not类型/可计算函数 取决于表达式结果，列引用取决于其值类型
        // 二元位操作取决于返回值, cast取决于强转后类型, 二元逻辑、二元比较操作符、常量取决于返回值
        if (expression instanceof PrometheusUnaryNotPrefixOperation)
            exprType = expression.getExpectedValue();
        else if (expression instanceof PrometheusUnaryPrefixOperation)
            exprType = ((PrometheusUnaryPrefixOperation) expression).getExpression().getExpectedValue();
        else if (expression instanceof PrometheusColumnReference)
            exprType = ((PrometheusColumnReference) expression).getValue();
        else exprType = expression.getExpectedValue();
        return exprType;
    }

    @Override
    public boolean isScalarExpression() {
        return expr.isScalarExpression();
    }

}
