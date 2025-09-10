package com.fuzzy.prometheus.ast;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.util.BigDecimalUtil;
import com.fuzzy.prometheus.PrometheusSchema.CommonDataType;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;

@Slf4j
public abstract class PrometheusConstant implements PrometheusExpression {

    @Override
    public boolean isScalarExpression() {
        return true;
    }

    public boolean isInt() {
        return false;
    }

    public boolean isNull() {
        return false;
    }

    public boolean isBoolean() {
        return false;
    }

    public boolean isDouble() {
        return false;
    }

    public boolean isBigDecimal() {
        return false;
    }

    public boolean isNumber() {
        return false;
    }

    public abstract static class PrometheusNoPQSConstant extends PrometheusConstant {

        @Override
        public boolean asBooleanNotNull() {
            throw throwException();
        }

        private RuntimeException throwException() {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
            return null;
        }

        @Override
        public PrometheusConstant castAs(CommonDataType type) {
            throw throwException();
        }

        @Override
        public String castAsString() {
            throw throwException();

        }

        @Override
        public CommonDataType getType() {
            throw throwException();
        }

        @Override
        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
            throw throwException();
        }

    }

    public static class PrometheusDoubleConstant extends PrometheusConstant {
        public final static int scale = 7;
        private final double value;
        private final String stringRepresentation;

        public PrometheusDoubleConstant(double value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
        }

        public PrometheusDoubleConstant(double value) {
            this.value = value;
            this.stringRepresentation = Double.toString(value);
        }

        @Override
        public boolean isDouble() {
            return true;
        }

        @Override
        public boolean isNumber() {
            return true;
        }

        @Override
        public double getDouble() {
            return value;
        }

        @Override
        public long getInt() {
            return (long) Math.floor(value);
        }

        @Override
        public boolean asBooleanNotNull() {
            return false;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public String castAsString() {
            return String.valueOf(value);
        }

        @Override
        public CommonDataType getType() {
            return CommonDataType.DOUBLE;
        }

        @Override
        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
            if (rightVal.isNull()) {
                return PrometheusConstant.createNullConstant();
            } else if (rightVal.isDouble() || rightVal.isBoolean() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return castAs(CommonDataType.BIGDECIMAL).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public PrometheusConstant castAs(CommonDataType type) {
            try {
                switch (type) {
                    case INT:
                        return PrometheusConstant.createIntConstant((long) value);
                    case BOOLEAN:
                        return PrometheusConstant.createBoolean(
                                castAs(CommonDataType.BIGDECIMAL).getBigDecimalValue()
                                        .compareTo(new BigDecimal(0)) != 0);
                    case DOUBLE:
                        return this;
                    case BIGDECIMAL:
                        return PrometheusConstant.createBigDecimalConstant(new BigDecimal(stringRepresentation));
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                log.warn("数字转换格式错误");
                throw new IgnoreMeException();
            }
        }

        @Override
        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
            if (rightVal.isNull()) {
                return PrometheusConstant.createNullConstant();
            } else if (rightVal.isDouble() || rightVal.isBoolean() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return castAs(CommonDataType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class PrometheusIntConstant extends PrometheusConstant {

        private final long value;
        private final String stringRepresentation;
        private final CommonDataType dataType;

        public PrometheusIntConstant(long value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
            dataType = CommonDataType.INT;
        }

        public PrometheusIntConstant(long value) {
            this.value = value;
            dataType = CommonDataType.INT;
            this.stringRepresentation = String.valueOf(value);
        }

        public PrometheusIntConstant(boolean booleanValue) {
            this.value = booleanValue ? 1 : 0;
            this.stringRepresentation = booleanValue ? "TRUE" : "FALSE";
            this.dataType = CommonDataType.BOOLEAN;
        }

        @Override
        public boolean isInt() {
            return CommonDataType.INT.equals(dataType);
        }

        @Override
        public boolean isNumber() {
            return isInt();
        }

        @Override
        public boolean isBoolean() {
            return CommonDataType.BOOLEAN.equals(dataType);
        }

        @Override
        public long getInt() {
            switch (dataType) {
                case BOOLEAN:
                case INT:
                    return value;
                default:
                    throw new UnsupportedOperationException(String.format("PrometheusIntConstant不支持该数据类型:%s!", dataType));
            }
        }

        @Override
        public boolean asBooleanNotNull() {
            return isBoolean() && this.value == 1;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public String castAsString() {
            return String.valueOf(value);
        }

        @Override
        public CommonDataType getType() {
            return this.dataType;
        }

        private String getStringRepr() {
            return String.valueOf(value);
        }

        @Override
        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
            if (rightVal.isNull()) {
                return PrometheusConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return PrometheusConstant.createBoolean(asBooleanNotNull() == rightVal.asBooleanNotNull());
            } else if (rightVal.isInt() || rightVal.isDouble() || rightVal.isBigDecimal()) {
                return castAs(CommonDataType.BIGDECIMAL).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public PrometheusConstant castAs(CommonDataType type) {
            try {
                switch (type) {
                    case BOOLEAN:
                        return PrometheusConstant.createBoolean(value != 0);
                    case INT:
                        return this;
                    case DOUBLE:
                        return PrometheusConstant.createDoubleConstant(value);
                    case BIGDECIMAL:
                        return PrometheusConstant.createBigDecimalConstant(new BigDecimal(value));
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                log.warn("数字转换格式错误");
                throw new IgnoreMeException();
            }
        }

        @Override
        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
            if (rightVal.isNull()) {
                return PrometheusIntConstant.createNullConstant();
            } else if (rightVal.isInt() || rightVal.isDouble() || rightVal.isBoolean() || rightVal.isBigDecimal()) {
                return castAs(CommonDataType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class PrometheusNullConstant extends PrometheusConstant {

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public boolean asBooleanNotNull() {
            throw new UnsupportedOperationException(this.toString());
        }

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
            return PrometheusConstant.createNullConstant();
        }

        @Override
        public PrometheusConstant castAs(CommonDataType type) {
            return this;
        }

        @Override
        public String castAsString() {
            return "NULL";
        }

        @Override
        public CommonDataType getType() {
            return CommonDataType.NULL;
        }

        @Override
        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
            return this;
        }

    }

    public static class PrometheusBigDecimalConstant extends PrometheusConstant {
        private final BigDecimal value;
        private final String stringRepresentation;

        public PrometheusBigDecimalConstant(BigDecimal value) {
            this.value = new BigDecimal(value.toPlainString());
            if (isInt()) this.stringRepresentation = value.stripTrailingZeros().toPlainString();
            else this.stringRepresentation = value.toPlainString();
        }

        @Override
        public boolean isBigDecimal() {
            return true;
        }

        @Override
        public boolean isDouble() {
            return BigDecimalUtil.isDouble(value);
        }

        @Override
        public boolean isInt() {
            return !BigDecimalUtil.isDouble(value);
        }

        @Override
        public boolean isNumber() {
            return true;
        }

        @Override
        public BigDecimal getBigDecimalValue() {
            return value;
        }

        @Override
        public double getDouble() {
            return value.doubleValue();
        }

        @Override
        public long getInt() {
            return value.longValue();
        }

        @Override
        public boolean asBooleanNotNull() {
            return false;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public String castAsString() {
            return stringRepresentation;
        }

        @Override
        public CommonDataType getType() {
            return CommonDataType.BIGDECIMAL;
        }

        @Override
        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
            if (rightVal.isNull()) {
                return PrometheusConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return PrometheusConstant.createFalse();
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return PrometheusConstant.createBoolean(value.subtract(
                                rightVal.castAs(CommonDataType.BIGDECIMAL).getBigDecimalValue())
                        .abs().compareTo(BigDecimal.valueOf(Math.pow(10, -PrometheusConstant.PrometheusDoubleConstant.scale))) <= 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public PrometheusConstant castAs(CommonDataType type) {
            try {
                switch (type) {
                    case INT:
                        return PrometheusConstant.createIntConstant(value.intValue());
                    case BOOLEAN:
                        return PrometheusConstant.createBoolean(value.compareTo(new BigDecimal(0)) != 0);
                    case DOUBLE:
                        return PrometheusConstant.createDoubleConstant(value.doubleValue());
                    case BIGDECIMAL:
                        return this;
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                log.warn("数字转换格式错误");
                throw new IgnoreMeException();
            }
        }

        @Override
        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
            if (rightVal.isNull()) {
                return PrometheusConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return PrometheusConstant.createFalse();
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return PrometheusConstant.createBoolean(value.compareTo(
                        rightVal.castAs(CommonDataType.BIGDECIMAL).getBigDecimalValue()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public double getDouble() {
        throw new UnsupportedOperationException();
    }

    public long getInt() {
        throw new UnsupportedOperationException();
    }

    public String getString() {
        throw new UnsupportedOperationException();
    }

    public static PrometheusConstant createNullConstant() {
        // TODO 空值问题
        return new PrometheusNullConstant();
    }

    public static PrometheusConstant createIntConstant(long value) {
        return new PrometheusIntConstant(value);
    }

    public static PrometheusConstant createBooleanIntConstant(boolean value) {
        return new PrometheusIntConstant(value);
    }

    public BigDecimal getBigDecimalValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrometheusConstant getExpectedValue() {
        return this;
    }

    public abstract boolean asBooleanNotNull();

    public abstract String getTextRepresentation();

    public static PrometheusConstant createFalse() {
        return PrometheusConstant.createBooleanIntConstant(false);
    }

    public static PrometheusConstant createBoolean(boolean isTrue) {
        return PrometheusConstant.createBooleanIntConstant(isTrue);
    }

    public static PrometheusConstant createTrue() {
        return PrometheusConstant.createBooleanIntConstant(true);
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract PrometheusConstant isEquals(PrometheusConstant rightVal);

    public abstract PrometheusConstant castAs(CommonDataType type);

    public abstract String castAsString();

    public abstract CommonDataType getType();

    protected abstract PrometheusConstant isLessThan(PrometheusConstant rightVal);

    public static PrometheusConstant createBigDecimalConstant(BigDecimal value) {
        return new PrometheusConstant.PrometheusBigDecimalConstant(value);
    }

    public static PrometheusConstant createDoubleConstant(double value) {
        return new PrometheusConstant.PrometheusDoubleConstant(value);
    }

    public static BigDecimal createFloatArithmeticTolerance() {
        return BigDecimal.valueOf(Math.pow(10, -PrometheusConstant.PrometheusDoubleConstant.scale));
    }
}
