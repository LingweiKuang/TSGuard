package com.fuzzy.prometheus.apiEntry;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

@Data
public class PrometheusDeleteParam {
    private final String databaseName;
    private final Long start;
    private final Long end;

    public PrometheusDeleteParam(String databaseName, Long start, Long end) {
        this.databaseName = databaseName;
        this.start = start;
        this.end = end;
    }

    /**
     * 生成 JSON 格式查询参数
     *
     * @param type
     * @return
     */
    public String genPrometheusRequestParam(PrometheusRequestType type) {
        return JSONObject.toJSONString(new PrometheusRequestParam(type, JSONObject.toJSONString(this)));
    }

    public String getSelector() {
        return String.format("match[]={__name__=\"%s\"}", this.databaseName);
    }

    public long getRcf3339Start() {
        // start=<rfc3339 | unix_timestamp>  unit: s
        return start / 1000;
    }

    public long getRcf3339End() {
        // end=<rfc3339 | unix_timestamp>  unit: s
        return end / 1000;
    }
}
