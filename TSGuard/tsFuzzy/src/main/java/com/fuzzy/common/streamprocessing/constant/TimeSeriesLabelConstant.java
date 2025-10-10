package com.fuzzy.common.streamprocessing.constant;

public enum TimeSeriesLabelConstant {
    DATABASE("database"),
    DATABASE_INIT("databaseInit"),
    TABLE("table"),
    TIME_SERIES("timeSeries"),
    METRIC_KEY("__name__");

    private final String label;

    TimeSeriesLabelConstant(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }
}
