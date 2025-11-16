package com.fuzzy.victoriametrics.apientry.requesthandler;

import com.benchmark.util.HttpClientUtils;
import com.benchmark.util.HttpRequestEnum;
import com.fuzzy.victoriametrics.apientry.VMApiEntry;

public class DataInsertRequestHandler implements RequestHandler {
    private static final String URI_PATH = "/api/v2/write";
    private static final String FORCE_FLUSH_URI_PATH = "/internal/force_flush";

    @Override
    public String execute(VMApiEntry apiEntry, String body) {
        String url = apiEntry.getTargetServer() + URI_PATH;
        HttpClientUtils.sendRequest(url, body, null, HttpRequestEnum.POST);
        String forceFlushUrl = apiEntry.getTargetServer() + FORCE_FLUSH_URI_PATH;
        HttpClientUtils.sendRequest(forceFlushUrl, null, null, HttpRequestEnum.GET);
        return "OK.";
    }
}
