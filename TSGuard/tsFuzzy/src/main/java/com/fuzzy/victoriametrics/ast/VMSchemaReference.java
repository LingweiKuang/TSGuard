package com.fuzzy.victoriametrics.ast;


import com.fuzzy.victoriametrics.VMSchema;

public class VMSchemaReference implements VMExpression {

    private final VMSchema schema;

    public VMSchemaReference(VMSchema schema) {
        this.schema = schema;
    }

    public VMSchema getSchema() {
        return schema;
    }

}
