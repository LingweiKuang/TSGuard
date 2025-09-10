package com.fuzzy.prometheus.ast;

public interface PrometheusExpression {

    default PrometheusConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

    default void checkSyntax() {
    }

    /**
     * 判断表达式是否为标量表达式
     *
     * @return
     */
    default boolean isScalarExpression() {
        return false;
    }

}
