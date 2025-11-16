package com.fuzzy.victoriametrics.ast;


import com.fuzzy.common.ast.SelectBase;

import java.util.Collections;
import java.util.List;

public class VMSelect extends SelectBase<VMExpression> implements VMExpression {

    private SelectType fromOptions = SelectType.ALL;
    private List<String> modifiers = Collections.emptyList();
//    private VMText hint;

    public enum SelectType {
        DISTINCT, ALL;
    }

    public void setSelectType(SelectType fromOptions) {
        this.setFromOptions(fromOptions);
    }

    public SelectType getFromOptions() {
        return fromOptions;
    }

    public void setFromOptions(SelectType fromOptions) {
        this.fromOptions = fromOptions;
    }

    public void setModifiers(List<String> modifiers) {
        this.modifiers = modifiers;
    }

    public List<String> getModifiers() {
        return modifiers;
    }

    @Override
    public VMConstant getExpectedValue() {
        return null;
    }
}
