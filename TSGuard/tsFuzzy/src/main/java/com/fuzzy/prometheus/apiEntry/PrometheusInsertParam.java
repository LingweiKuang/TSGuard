package com.fuzzy.prometheus.apiEntry;

import build.buf.gen.io.prometheus.write.v2.Request;
import com.alibaba.fastjson.JSONObject;
import com.fuzzy.prometheus.apiEntry.entity.CollectorAttribute;
import com.google.protobuf.ByteString;
import lombok.Data;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Data
public class PrometheusInsertParam {
    // <MetricName, Collector>
    Map<String, CollectorAttribute> collectorMap;

    public PrometheusInsertParam() {
        this.collectorMap = new HashMap<>();
    }

    public byte[] snappyCompressedRequest(String metricName) throws IOException {
        // 序列化为 Protobuf 字节并压缩
        ByteString data = createRequestForRemoteWrite(metricName).toByteString();
        return Snappy.compress(data.toByteArray());
    }

    public Request createRequestForRemoteWrite(String metricName) {
        return collectorMap.get(metricName).createRequestForRemoteWrite();
    }

    public String genPrometheusQueryParam() {
        return JSONObject.toJSONString(new PrometheusRequestParam(PrometheusRequestType.PUSH_DATA,
                JSONObject.toJSONString(this)));
    }
}
