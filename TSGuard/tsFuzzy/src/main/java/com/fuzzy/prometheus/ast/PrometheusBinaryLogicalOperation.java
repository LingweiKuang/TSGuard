package com.fuzzy.prometheus.ast;


import com.fuzzy.Randomly;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrometheusBinaryLogicalOperation implements PrometheusExpression {

    private final PrometheusExpression left;
    private final PrometheusExpression right;
    private final PrometheusBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum PrometheusBinaryLogicalOperator {
        AND("AND") {
            @Override
            public PrometheusConstant apply(PrometheusConstant left, PrometheusConstant right) {
                // TODO 矢量取交集
                if (left.isNull() || right.isNull()) return PrometheusConstant.createNullConstant();
                else return PrometheusConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
            }
        },
        OR("OR") {
            @Override
            public PrometheusConstant apply(PrometheusConstant left, PrometheusConstant right) {
                // TODO 矢量取并集
                if (left.isNull() && right.isNull()) return PrometheusConstant.createNullConstant();
                else if (left.isNull()) return PrometheusConstant.createBoolean(right.asBooleanNotNull());
                else if (right.isNull()) return PrometheusConstant.createBoolean(left.asBooleanNotNull());
                else return PrometheusConstant.createBoolean(left.asBooleanNotNull() || right.asBooleanNotNull());
            }
        },
        UNLESS("UNLESS") {
            @Override
            public PrometheusConstant apply(PrometheusConstant left, PrometheusConstant right) {
                // TODO 矢量取补集
                return PrometheusConstant.createBoolean(true);
            }
        };

        private final String[] textRepresentations;

        PrometheusBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract PrometheusConstant apply(PrometheusConstant left, PrometheusConstant right);

        public static PrometheusBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public static PrometheusBinaryLogicalOperator getOperatorByText(String text) {
            for (PrometheusBinaryLogicalOperator value : PrometheusBinaryLogicalOperator.values()) {
                for (String textRepresentation : value.textRepresentations) {
                    if (textRepresentation.equalsIgnoreCase(text)) return value;
                }
            }
            throw new UnsupportedOperationException();
        }
    }

    public PrometheusBinaryLogicalOperation(PrometheusExpression left, PrometheusExpression right, PrometheusBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    public PrometheusExpression getLeft() {
        return left;
    }

    public PrometheusBinaryLogicalOperator getOp() {
        return op;
    }

    public PrometheusExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public PrometheusConstant getExpectedValue() {
        // TODO 矢量运算 -> 返回值为集合
        // TODO PQS 不支持通过该表达式获取结果
        return left.getExpectedValue();
//        PrometheusConstant leftExpected = left.getExpectedValue();
//        PrometheusConstant rightExpected = right.getExpectedValue();
//        if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
//            return null;
//        }
//        return op.apply(leftExpected, rightExpected);
    }

    @Override
    public boolean isScalarExpression() {
        return left.isScalarExpression() && right.isScalarExpression();
    }

    @Override
    public void checkSyntax() {
//        if (!(left.getExpectedValue().isBoolean() && right.getExpectedValue().isBoolean())) {
//            log.warn("dataType is not BOOL");
//            throw new ReGenerateExpressionException("Logic");
//        }
    }
}
