package com.fuzzy.victoriametrics.ast;


import com.fuzzy.victoriametrics.VMSchema.VMColumn;

public class VMColumnReference implements VMExpression {

    private final VMColumn column;
    private final VMConstant value;

    public VMColumnReference(VMColumn column, VMConstant value) {
        this.column = column;
        this.value = value;
    }

    public static VMColumnReference create(VMColumn column, VMConstant value) {
        return new VMColumnReference(column, value);
    }

    public VMColumn getColumn() {
        return column;
    }

    public VMConstant getValue() {
        return value;
    }

    @Override
    public VMConstant getExpectedValue() {
        return value;
    }

    @Override
    public boolean isScalarExpression() {
        return false;
    }

}
