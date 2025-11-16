package com.fuzzy.victoriametrics.apientry;

import com.fuzzy.victoriametrics.apientry.requesthandler.*;

public class VMRequestFactory {

    public static RequestHandler getHandler(VMRequestType type) {
        return switch (type) {
            case INSERT_DATA -> new DataInsertRequestHandler();
            case SERIES_QUERY -> new SeriesQueryHandler();
            case SERIES_DELETE -> new SeriesDeleteHandler();
            case INSTANT_QUERY -> new InstantQueryHandler();
            case RANGE_QUERY -> new RangeQueryHandler();
            default -> null;
        };
    }
}
