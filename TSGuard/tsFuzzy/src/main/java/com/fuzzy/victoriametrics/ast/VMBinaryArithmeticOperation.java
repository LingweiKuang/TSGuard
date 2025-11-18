package com.fuzzy.victoriametrics.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.common.util.BigDecimalUtil;
import com.fuzzy.victoriametrics.VMSchema;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.BinaryOperator;

@Slf4j
public class VMBinaryArithmeticOperation implements VMExpression {

    private final VMExpression left;
    private final VMExpression right;
    private final VMBinaryArithmeticOperator op;

    public enum VMBinaryArithmeticOperator {
        PLUS("+") {
            @Override
            public VMConstant apply(VMConstant left, VMConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.add(r)
                        .setScale(TimeSeriesStream.ARITHMETIC_PRECISION, RoundingMode.HALF_UP));
            }
        },
        SUBTRACT("-") {
            @Override
            public VMConstant apply(VMConstant left, VMConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.subtract(r)
                        .setScale(TimeSeriesStream.ARITHMETIC_PRECISION, RoundingMode.HALF_UP));
            }
        },
        MULTIPLY("*") {
            @Override
            public VMConstant apply(VMConstant left, VMConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.multiply(r)
                        .setScale(TimeSeriesStream.ARITHMETIC_PRECISION, RoundingMode.HALF_UP));
            }
        },
        DIVIDE("/") {
            @Override
            public VMConstant apply(VMConstant left, VMConstant right) {
                return applyArithmeticOperation(left, right, (l, r) ->
                        l.divide(r, TimeSeriesStream.ARITHMETIC_PRECISION, RoundingMode.HALF_UP));
            }
        },
        MODULO("%") {
            @Override
            public VMConstant apply(VMConstant left, VMConstant right) {
                // 小数求模
                if (left.castAs(VMSchema.CommonDataType.BIGDECIMAL).getBigDecimalValue()
                        .remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0
                        || right.castAs(VMSchema.CommonDataType.BIGDECIMAL).getBigDecimalValue()
                        .remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0) {
                    log.warn("不支持小数求模");
                    throw new IgnoreMeException();
                }

                // TODO
                return applyArithmeticOperation(left, right, (l, r) -> l.remainder(r));
            }
        },
        ATAN2("atan2") {
            @Override
            public VMConstant apply(VMConstant left, VMConstant right) {
                return applyArithmeticOperation(left, right, (l, r) ->
                        BigDecimalUtil.atan2(l, r, TimeSeriesStream.ARITHMETIC_PRECISION));
            }
        };

        private String textRepresentation;

        private static VMConstant applyArithmeticOperation(VMConstant left, VMConstant right,
                                                           BinaryOperator<BigDecimal> op) {
            if (left.isNull() || right.isNull()) {
                return VMConstant.createNullConstant();
            } else {
                BigDecimal leftVal = left.castAs(VMSchema.CommonDataType.BIGDECIMAL).getBigDecimalValue();
                BigDecimal rightVal = right.castAs(VMSchema.CommonDataType.BIGDECIMAL).getBigDecimalValue();

                try {
                    BigDecimal value = op.apply(leftVal, rightVal);
                    return VMConstant.createBigDecimalConstant(value);
                } catch (ArithmeticException e) {
                    log.warn("除数不能为0.");
                    throw new IgnoreMeException();
                }
            }
        }

        VMBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract VMConstant apply(VMConstant left, VMConstant right);

        public static VMBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(PLUS, SUBTRACT, MULTIPLY, DIVIDE, ATAN2);
        }

        public static VMBinaryArithmeticOperator getRandomOperatorForTimestamp() {
            return Randomly.fromOptions(VMBinaryArithmeticOperator.PLUS,
                    VMBinaryArithmeticOperator.SUBTRACT);
        }

        public static VMBinaryArithmeticOperator getOperatorByTextRepresentation(String textRepresentation) {
            for (VMBinaryArithmeticOperator value : VMBinaryArithmeticOperator.values()) {
                if (value.getTextRepresentation().equals(textRepresentation)) {
                    return value;
                }
            }
            throw new IllegalArgumentException("Invalid binary arithmetic operator: " + textRepresentation);
        }
    }

    public VMBinaryArithmeticOperation(VMExpression left, VMExpression right, VMBinaryArithmeticOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public void checkSyntax() {
//        if ((left instanceof VMColumnReference
//                && ((VMColumnReference) left).getColumn().getName()
//                .equalsIgnoreCase(VMConstantString.TIME_FIELD_NAME.getName())
//                && right instanceof VMColumnReference
//                && ((VMColumnReference) right).getColumn().getName()
//                .equalsIgnoreCase(VMConstantString.TIME_FIELD_NAME.getName()))) {
//            log.warn("Invalid operation");
//            throw new ReGenerateExpressionException("Between");
//        }
    }

    @Override
    public VMConstant getExpectedValue() {
        VMConstant leftExpected = left.getExpectedValue();
        VMConstant rightExpected = right.getExpectedValue();
        return op.apply(leftExpected, rightExpected);
    }

    @Override
    public boolean isScalarExpression() {
        return left.isScalarExpression() && right.isScalarExpression();
    }

    public VMExpression getLeft() {
        return left;
    }

    public VMBinaryArithmeticOperator getOp() {
        return op;
    }

    public VMExpression getRight() {
        return right;
    }

}
