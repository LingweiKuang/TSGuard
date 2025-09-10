package com.fuzzy.prometheus.ast;


import com.fuzzy.prometheus.PrometheusSchema.PrometheusColumn;

public class PrometheusColumnReference implements PrometheusExpression {

    private final PrometheusColumn column;
    private final PrometheusConstant value;

    public PrometheusColumnReference(PrometheusColumn column, PrometheusConstant value) {
        this.column = column;
        this.value = value;
    }

    public static PrometheusColumnReference create(PrometheusColumn column, PrometheusConstant value) {
        return new PrometheusColumnReference(column, value);
    }

    public PrometheusColumn getColumn() {
        return column;
    }

    public PrometheusConstant getValue() {
        return value;
    }

    @Override
    public PrometheusConstant getExpectedValue() {
        return value;
    }

    @Override
    public boolean isScalarExpression() {
        return false;
    }

}
