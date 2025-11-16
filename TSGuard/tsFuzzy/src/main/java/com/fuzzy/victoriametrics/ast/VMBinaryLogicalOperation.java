package com.fuzzy.victoriametrics.ast;


import com.fuzzy.Randomly;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VMBinaryLogicalOperation implements VMExpression {

    private final VMExpression left;
    private final VMExpression right;
    private final VMBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum VMBinaryLogicalOperator {
        AND("AND") {
            @Override
            public VMConstant apply(VMConstant left, VMConstant right) {
                // TODO 矢量取交集
                if (left.isNull() || right.isNull()) return VMConstant.createNullConstant();
                else return VMConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
            }
        },
        OR("OR") {
            @Override
            public VMConstant apply(VMConstant left, VMConstant right) {
                // TODO 矢量取并集
                if (left.isNull() && right.isNull()) return VMConstant.createNullConstant();
                else if (left.isNull()) return VMConstant.createBoolean(right.asBooleanNotNull());
                else if (right.isNull()) return VMConstant.createBoolean(left.asBooleanNotNull());
                else return VMConstant.createBoolean(left.asBooleanNotNull() || right.asBooleanNotNull());
            }
        },
        UNLESS("UNLESS") {
            @Override
            public VMConstant apply(VMConstant left, VMConstant right) {
                // TODO 矢量取补集
                return VMConstant.createBoolean(true);
            }
        };

        private final String[] textRepresentations;

        VMBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract VMConstant apply(VMConstant left, VMConstant right);

        public static VMBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public static VMBinaryLogicalOperator getOperatorByText(String text) {
            for (VMBinaryLogicalOperator value : VMBinaryLogicalOperator.values()) {
                for (String textRepresentation : value.textRepresentations) {
                    if (textRepresentation.equalsIgnoreCase(text)) return value;
                }
            }
            throw new UnsupportedOperationException();
        }
    }

    public VMBinaryLogicalOperation(VMExpression left, VMExpression right, VMBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    public VMExpression getLeft() {
        return left;
    }

    public VMBinaryLogicalOperator getOp() {
        return op;
    }

    public VMExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public VMConstant getExpectedValue() {
        // TODO 矢量运算 -> 返回值为集合
        // TODO PQS 不支持通过该表达式获取结果
        return left.getExpectedValue();
    }

    @Override
    public boolean isScalarExpression() {
        return left.isScalarExpression() && right.isScalarExpression();
    }
}
