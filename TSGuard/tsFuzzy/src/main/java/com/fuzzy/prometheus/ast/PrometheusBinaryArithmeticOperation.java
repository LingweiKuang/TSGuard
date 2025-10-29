package com.fuzzy.prometheus.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.common.util.BigDecimalUtil;
import com.fuzzy.prometheus.PrometheusSchema;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.BinaryOperator;

@Slf4j
public class PrometheusBinaryArithmeticOperation implements PrometheusExpression {

    private final PrometheusExpression left;
    private final PrometheusExpression right;
    private final PrometheusBinaryArithmeticOperator op;

    public enum PrometheusBinaryArithmeticOperator {
        PLUS("+") {
            @Override
            public PrometheusConstant apply(PrometheusConstant left, PrometheusConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.add(r)
                        .setScale(TimeSeriesStream.ARITHMETIC_PRECISION, RoundingMode.HALF_UP));
            }
        },
        SUBTRACT("-") {
            @Override
            public PrometheusConstant apply(PrometheusConstant left, PrometheusConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.subtract(r)
                        .setScale(TimeSeriesStream.ARITHMETIC_PRECISION, RoundingMode.HALF_UP));
            }
        },
        MULTIPLY("*") {
            @Override
            public PrometheusConstant apply(PrometheusConstant left, PrometheusConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.multiply(r)
                        .setScale(TimeSeriesStream.ARITHMETIC_PRECISION, RoundingMode.HALF_UP));
            }
        },
        DIVIDE("/") {
            @Override
            public PrometheusConstant apply(PrometheusConstant left, PrometheusConstant right) {
                return applyArithmeticOperation(left, right, (l, r) ->
                        l.divide(r, TimeSeriesStream.ARITHMETIC_PRECISION, RoundingMode.HALF_UP));
            }
        },
        MODULO("%") {
            @Override
            public PrometheusConstant apply(PrometheusConstant left, PrometheusConstant right) {
                // 小数求模
                if (left.castAs(PrometheusSchema.CommonDataType.BIGDECIMAL).getBigDecimalValue()
                        .remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0
                        || right.castAs(PrometheusSchema.CommonDataType.BIGDECIMAL).getBigDecimalValue()
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
            public PrometheusConstant apply(PrometheusConstant left, PrometheusConstant right) {
                return applyArithmeticOperation(left, right, (l, r) ->
                        BigDecimalUtil.atan2(l, r, TimeSeriesStream.ARITHMETIC_PRECISION));
            }
        };

        private String textRepresentation;

        private static PrometheusConstant applyArithmeticOperation(PrometheusConstant left, PrometheusConstant right,
                                                                   BinaryOperator<BigDecimal> op) {
            if (left.isNull() || right.isNull()) {
                return PrometheusConstant.createNullConstant();
            } else {
                BigDecimal leftVal = left.castAs(PrometheusSchema.CommonDataType.BIGDECIMAL).getBigDecimalValue();
                BigDecimal rightVal = right.castAs(PrometheusSchema.CommonDataType.BIGDECIMAL).getBigDecimalValue();

                try {
                    BigDecimal value = op.apply(leftVal, rightVal);
                    return PrometheusConstant.createBigDecimalConstant(value);
                } catch (ArithmeticException e) {
                    log.warn("除数不能为0.");
                    throw new IgnoreMeException();
                }
            }
        }

        PrometheusBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract PrometheusConstant apply(PrometheusConstant left, PrometheusConstant right);

        public static PrometheusBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public static PrometheusBinaryArithmeticOperator getRandomOperatorForTimestamp() {
            return Randomly.fromOptions(PrometheusBinaryArithmeticOperator.PLUS,
                    PrometheusBinaryArithmeticOperator.SUBTRACT);
        }

        public static PrometheusBinaryArithmeticOperator getOperatorByTextRepresentation(String textRepresentation) {
            for (PrometheusBinaryArithmeticOperator value : PrometheusBinaryArithmeticOperator.values()) {
                if (value.getTextRepresentation().equals(textRepresentation)) {
                    return value;
                }
            }
            throw new IllegalArgumentException("Invalid binary arithmetic operator: " + textRepresentation);
        }
    }

    public PrometheusBinaryArithmeticOperation(PrometheusExpression left, PrometheusExpression right, PrometheusBinaryArithmeticOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public void checkSyntax() {
//        if ((left instanceof PrometheusColumnReference
//                && ((PrometheusColumnReference) left).getColumn().getName()
//                .equalsIgnoreCase(PrometheusConstantString.TIME_FIELD_NAME.getName())
//                && right instanceof PrometheusColumnReference
//                && ((PrometheusColumnReference) right).getColumn().getName()
//                .equalsIgnoreCase(PrometheusConstantString.TIME_FIELD_NAME.getName()))) {
//            log.warn("Invalid operation");
//            throw new ReGenerateExpressionException("Between");
//        }
    }

    @Override
    public PrometheusConstant getExpectedValue() {
        PrometheusConstant leftExpected = left.getExpectedValue();
        PrometheusConstant rightExpected = right.getExpectedValue();
        return op.apply(leftExpected, rightExpected);
    }

    @Override
    public boolean isScalarExpression() {
        return left.isScalarExpression() && right.isScalarExpression();
    }

    public PrometheusExpression getLeft() {
        return left;
    }

    public PrometheusBinaryArithmeticOperator getOp() {
        return op;
    }

    public PrometheusExpression getRight() {
        return right;
    }

}
