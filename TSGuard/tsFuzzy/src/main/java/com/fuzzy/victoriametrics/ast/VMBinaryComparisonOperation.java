package com.fuzzy.victoriametrics.ast;


import com.fuzzy.Randomly;

public class VMBinaryComparisonOperation implements VMExpression {

    public enum BinaryComparisonOperator {
        EQUALS("==") {
            @Override
            public VMConstant getExpectedValue(VMConstant leftVal, VMConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public VMConstant getExpectedValue(VMConstant leftVal, VMConstant rightVal) {
                VMConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isNull()) return VMConstant.createNullConstant();
                return VMConstant.createBoolean(!isEquals.asBooleanNotNull());
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        LESS("<") {
            @Override
            public VMConstant getExpectedValue(VMConstant leftVal, VMConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return GREATER;
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public VMConstant getExpectedValue(VMConstant leftVal, VMConstant rightVal) {
                VMConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isNull()) return VMConstant.createNullConstant();
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
            public VMConstant getExpectedValue(VMConstant leftVal, VMConstant rightVal) {
                VMConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return VMConstant.createNullConstant();
                if (equals.asBooleanNotNull()) {
                    return VMConstant.createFalse();
                } else {
                    VMConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return VMConstant.createNullConstant();
                    return VMConstant.createBoolean(!applyLess.asBooleanNotNull());
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return LESS;
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public VMConstant getExpectedValue(VMConstant leftVal, VMConstant rightVal) {
                VMConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return VMConstant.createNullConstant();
                if (equals.asBooleanNotNull()) {
                    return VMConstant.createTrue();
                } else {
                    VMConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return VMConstant.createNullConstant();
                    return VMConstant.createBoolean(!applyLess.asBooleanNotNull());
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

        public abstract VMConstant getExpectedValue(VMConstant leftVal, VMConstant rightVal);

        public static BinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(BinaryComparisonOperator.values());
        }

        public abstract BinaryComparisonOperator reverseInequality();

        public static BinaryComparisonOperator parseBinaryComparisonOperator(String text) {
            for (BinaryComparisonOperator value : BinaryComparisonOperator.values()) {
                if (value.getTextRepresentation().equals(text)) {
                    return value;
                }
            }
            throw new IllegalArgumentException("Invalid binary comparison operator: " + text);
        }
    }

    private final VMExpression left;
    private final VMExpression right;
    private final BinaryComparisonOperator op;

    public VMBinaryComparisonOperation(VMExpression left, VMExpression right, BinaryComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public VMExpression getLeft() {
        return left;
    }

    public BinaryComparisonOperator getOp() {
        return op;
    }

    public VMExpression getRight() {
        return right;
    }

    @Override
    public VMConstant getExpectedValue() {
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
