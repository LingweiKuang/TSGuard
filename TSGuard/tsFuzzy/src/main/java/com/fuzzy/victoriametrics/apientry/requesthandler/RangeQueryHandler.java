package com.fuzzy.victoriametrics.apientry.requesthandler;

import com.benchmark.util.HttpClientUtils;
import com.benchmark.util.HttpRequestEnum;
import com.fuzzy.victoriametrics.apientry.VMApiEntry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RangeQueryHandler implements RequestHandler {
    private static final String URI_PATH = "/api/v1/query_range";

    @Override
    public String execute(VMApiEntry apiEntry, String body) {
        String url = apiEntry.getTargetServer() + URI_PATH;
//        VMRangeQueryRequest request = JSONObject.parseObject(body, VMRangeQueryRequest.class);
//        log.info("curl: curl http://localhost:8428/prometheus/api/v1/query_range -d 'query={}'" +
//                        " -d 'start={}' -d 'step={}' -d 'end={}'",
//                request.getQuery(), request.getStart(), request.getStep(), request.getEnd());
        return HttpClientUtils.sendRequest(url, body, null, HttpRequestEnum.POST_WITH_URLENCODED);
    }
}
