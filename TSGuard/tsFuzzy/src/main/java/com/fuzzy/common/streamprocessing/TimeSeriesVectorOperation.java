package com.fuzzy.common.streamprocessing;

import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;

public interface TimeSeriesVectorOperation {
    TimeSeriesStream.TimeSeriesVector and(TimeSeriesStream.TimeSeriesVector rightVector);

    TimeSeriesStream.TimeSeriesVector or(TimeSeriesStream.TimeSeriesVector rightVector);

    TimeSeriesStream.TimeSeriesVector unless(TimeSeriesStream.TimeSeriesVector rightVector);
}
