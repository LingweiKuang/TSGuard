package com.fuzzy.prometheus.ast;


import com.fuzzy.Randomly;

public class PrometheusBinaryComparisonOperation implements PrometheusExpression {

    public enum BinaryComparisonOperator {
        EQUALS("==") {
            @Override
            public PrometheusConstant getExpectedValue(PrometheusConstant leftVal, PrometheusConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public PrometheusConstant getExpectedValue(PrometheusConstant leftVal, PrometheusConstant rightVal) {
                PrometheusConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isNull()) return PrometheusConstant.createNullConstant();
                return PrometheusConstant.createBoolean(!isEquals.asBooleanNotNull());
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        LESS("<") {
            @Override
            public PrometheusConstant getExpectedValue(PrometheusConstant leftVal, PrometheusConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return GREATER;
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public PrometheusConstant getExpectedValue(PrometheusConstant leftVal, PrometheusConstant rightVal) {
                PrometheusConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isNull()) return PrometheusConstant.createNullConstant();
                if (!lessThan.asBooleanNotNull()) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return GREATER_EQUALS;
            }
        },
        GREATER(">") {
            @Override
            public PrometheusConstant getExpectedValue(PrometheusConstant leftVal, PrometheusConstant rightVal) {
                PrometheusConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return PrometheusConstant.createNullConstant();
                if (equals.asBooleanNotNull()) {
                    return PrometheusConstant.createFalse();
                } else {
                    PrometheusConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return PrometheusConstant.createNullConstant();
                    return PrometheusConstant.createBoolean(!applyLess.asBooleanNotNull());
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return LESS;
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public PrometheusConstant getExpectedValue(PrometheusConstant leftVal, PrometheusConstant rightVal) {
                PrometheusConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return PrometheusConstant.createNullConstant();
                if (equals.asBooleanNotNull()) {
                    return PrometheusConstant.createTrue();
                } else {
                    PrometheusConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return PrometheusConstant.createNullConstant();
                    return PrometheusConstant.createBoolean(!applyLess.asBooleanNotNull());
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return LESS_EQUALS;
            }
        };

        private final String[] textRepresentations;

        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        BinaryComparisonOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract PrometheusConstant getExpectedValue(PrometheusConstant leftVal, PrometheusConstant rightVal);

        public static BinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(BinaryComparisonOperator.values());
        }

        public abstract BinaryComparisonOperator reverseInequality();
    }

    private final PrometheusExpression left;
    private final PrometheusExpression right;
    private final BinaryComparisonOperator op;

    public PrometheusBinaryComparisonOperation(PrometheusExpression left, PrometheusExpression right, BinaryComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public PrometheusExpression getLeft() {
        return left;
    }

    public BinaryComparisonOperator getOp() {
        return op;
    }

    public PrometheusExpression getRight() {
        return right;
    }

    @Override
    public PrometheusConstant getExpectedValue() {
        return op.getExpectedValue(left.getExpectedValue(), right.getExpectedValue());
    }

    @Override
    public boolean isScalarExpression() {
        return left.isScalarExpression() && right.isScalarExpression();
    }

    public boolean containsEqual() {
        return BinaryComparisonOperator.EQUALS.equals(this.op)
                || BinaryComparisonOperator.GREATER_EQUALS.equals(this.op)
                || BinaryComparisonOperator.LESS_EQUALS.equals(this.op);
    }
}
