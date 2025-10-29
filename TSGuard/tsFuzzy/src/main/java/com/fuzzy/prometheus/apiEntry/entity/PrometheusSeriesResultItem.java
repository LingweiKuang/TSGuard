package com.fuzzy.prometheus.apiEntry.entity;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.annotation.JSONField;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusColumn;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusDataType;
import lombok.Data;

import java.util.Objects;

@Data
public class PrometheusSeriesResultItem {
    @JSONField(name = "__name__")
    private String database;
    @JSONField(name = "table")
    private String table;
    @JSONField(name = "timeSeries")
    private String column;

    public PrometheusColumn transToColumn() {
        // 列类型的确定
        PrometheusDataType dataType;
        if (column.endsWith(TimeSeriesLabelConstant.END_WITH_COUNTER.getLabel())) dataType = PrometheusDataType.COUNTER;
        else dataType = PrometheusDataType.GAUGE;
        return new PrometheusColumn(column, false, dataType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrometheusSeriesResultItem series = (PrometheusSeriesResultItem) o;
        return ObjectUtil.equals(this.database, series.database) && ObjectUtil.equals(this.table, series.table)
                && ObjectUtil.equals(this.column, series.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table, column);
    }
}
