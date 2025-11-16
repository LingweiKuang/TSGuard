package com.fuzzy.victoriametrics.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.ast.BinaryOperatorNode.Operator;
import com.fuzzy.common.ast.UnaryOperatorNode;

public class VMUnaryPrefixOperation extends UnaryOperatorNode<VMExpression, VMUnaryPrefixOperation.VMUnaryPrefixOperator>
        implements VMExpression {

    public enum VMUnaryPrefixOperator implements Operator {
        PLUS("+") {
            @Override
            public VMConstant applyNotNull(VMConstant expr) {
                return expr;
            }
        },
        MINUS("-") {
            @Override
            public VMConstant applyNotNull(VMConstant expr) {
                if (expr.isBoolean()) throw new IgnoreMeException();
                else if (expr.isInt()) {
                    return VMConstant.createIntConstant(-expr.getInt());
                } else if (expr.isDouble()) {
                    return VMConstant.createDoubleConstant(-expr.getDouble());
                } else if (expr.isBigDecimal()) {
                    return VMConstant.createBigDecimalConstant(expr.getBigDecimalValue().negate());
                } else throw new AssertionError(expr);

            }
        };

        private String[] textRepresentations;

        VMUnaryPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract VMConstant applyNotNull(VMConstant expr);

        public static VMUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public static VMUnaryPrefixOperator getVMUnaryPrefixOperator(String textRepresentation) {
            for (VMUnaryPrefixOperator value : VMUnaryPrefixOperator.values()) {
                if (value.getTextRepresentation().equals(textRepresentation)) return value;
            }
            throw new IllegalArgumentException("Invalid unary prefix operator: " + textRepresentation);
        }
    }

    public VMUnaryPrefixOperation(VMExpression expr, VMUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public VMConstant getExpectedValue() {
        VMConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return VMConstant.createNullConstant();
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
