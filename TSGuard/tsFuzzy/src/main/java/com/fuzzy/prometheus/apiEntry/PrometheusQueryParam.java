package com.fuzzy.prometheus.apiEntry;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PrometheusQueryParam {
    private String requestBody;
    private Long start;
    private Long end;
    private Long step;
    private Long limit;

    public PrometheusQueryParam(String requestBody, long start) {
        this.requestBody = requestBody;
        this.start = start;
    }

    public PrometheusQueryParam(String requestBody, long start, long end) {
        this.requestBody = requestBody;
        // Prometheus 支持以秒级精度进行数据查询，step = 1 捞出所有数据
        this.start = start / 1000;
        this.end = end / 1000;
        this.step = 1L;
    }

    public PrometheusQueryParam(String requestBody) {
        this.requestBody = requestBody;
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
}
