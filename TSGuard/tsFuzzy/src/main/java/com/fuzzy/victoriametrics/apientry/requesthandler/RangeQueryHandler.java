package com.fuzzy.victoriametrics.apientry.requesthandler;

import com.benchmark.util.HttpClientUtils;
import com.benchmark.util.HttpRequestEnum;
import com.fuzzy.victoriametrics.apientry.VMApiEntry;

public class RangeQueryHandler implements RequestHandler {
    private static final String URI_PATH = "/api/v1/query_range";

    @Override
    public String execute(VMApiEntry apiEntry, String body) {
        String url = apiEntry.getTargetServer() + URI_PATH;
        return HttpClientUtils.sendRequest(url, body, null, HttpRequestEnum.POST_WITH_URLENCODED);
    }
}
