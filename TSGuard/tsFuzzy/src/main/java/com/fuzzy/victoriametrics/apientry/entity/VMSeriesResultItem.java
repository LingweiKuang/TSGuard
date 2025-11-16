package com.fuzzy.victoriametrics.apientry.entity;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.annotation.JSONField;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.victoriametrics.VMSchema;
import lombok.Data;

import java.util.Objects;

@Data
public class VMSeriesResultItem {
    @JSONField(name = "__name__")
    private String database;
    @JSONField(name = "table")
    private String table;
    @JSONField(name = "timeSeries")
    private String column;

    public VMSchema.VMColumn transToColumn() {
        // 列类型的确定
        VMSchema.VMDataType dataType;
        if (column.endsWith(TimeSeriesLabelConstant.END_WITH_COUNTER.getLabel()))
            dataType = VMSchema.VMDataType.COUNTER;
        else dataType = VMSchema.VMDataType.GAUGE;
        return new VMSchema.VMColumn(column, false, dataType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VMSeriesResultItem series = (VMSeriesResultItem) o;
        return ObjectUtil.equals(this.database, series.database) && ObjectUtil.equals(this.table, series.table)
                && ObjectUtil.equals(this.column, series.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table, column);
    }
}
