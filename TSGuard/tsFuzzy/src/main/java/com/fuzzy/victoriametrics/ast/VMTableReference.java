package com.fuzzy.victoriametrics.ast;


import com.fuzzy.victoriametrics.VMSchema;

public class VMTableReference implements VMExpression {

    private final VMSchema.VMTable table;

    public VMTableReference(VMSchema.VMTable table) {
        this.table = table;
    }

    public VMSchema.VMTable getTable() {
        return table;
    }

}
