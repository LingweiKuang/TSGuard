package com.fuzzy.prometheus.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.ast.BinaryOperatorNode.Operator;
import com.fuzzy.common.ast.UnaryOperatorNode;
import com.fuzzy.prometheus.ast.PrometheusUnaryPrefixOperation.PrometheusUnaryPrefixOperator;

public class PrometheusUnaryPrefixOperation extends UnaryOperatorNode<PrometheusExpression, PrometheusUnaryPrefixOperator>
        implements PrometheusExpression {

    public enum PrometheusUnaryPrefixOperator implements Operator {
        PLUS("+") {
            @Override
            public PrometheusConstant applyNotNull(PrometheusConstant expr) {
                return expr;
            }
        },
        MINUS("-") {
            @Override
            public PrometheusConstant applyNotNull(PrometheusConstant expr) {
                if (expr.isBoolean()) throw new IgnoreMeException();
                else if (expr.isInt()) {
                    return PrometheusConstant.createIntConstant(-expr.getInt());
                } else if (expr.isDouble()) {
                    return PrometheusConstant.createDoubleConstant(-expr.getDouble());
                } else if (expr.isBigDecimal()) {
                    return PrometheusConstant.createBigDecimalConstant(expr.getBigDecimalValue().negate());
                } else throw new AssertionError(expr);

            }
        };

        private String[] textRepresentations;

        PrometheusUnaryPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract PrometheusConstant applyNotNull(PrometheusConstant expr);

        public static PrometheusUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public PrometheusUnaryPrefixOperation(PrometheusExpression expr, PrometheusUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public PrometheusConstant getExpectedValue() {
        PrometheusConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return PrometheusConstant.createNullConstant();
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

    @Override
    public boolean isScalarExpression() {
        return expr.isScalarExpression();
    }

}
