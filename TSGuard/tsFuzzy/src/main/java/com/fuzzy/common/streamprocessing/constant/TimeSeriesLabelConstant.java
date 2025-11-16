package com.fuzzy.common.streamprocessing.constant;

public enum TimeSeriesLabelConstant {
    DATABASE("database"),
    DATABASE_INIT("databaseInit"),
    TABLE("table"),
    TIME_SERIES("timeSeries"),
    METRIC_KEY("__name__"),
    END_WITH_COUNTER("_counter"),
    END_WITH_GAUGE("_gauge"),
    END_WITH_VALUE("_value"),
    ;

    private final String label;

    TimeSeriesLabelConstant(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }
}
