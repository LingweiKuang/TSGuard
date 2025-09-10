package com.fuzzy.prometheus.apiEntry;

import com.benchmark.commonClass.ApiEntry;
import com.benchmark.constants.AggFunctionType;
import com.benchmark.dto.DBValParam;
import com.benchmark.entity.AggCountResult;
import com.benchmark.entity.DBVal;
import com.benchmark.entity.PerformanceEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

@Slf4j
public class PrometheusApiEntry implements ApiEntry {
    private String host;
    private int port;
    private int pushGatewayPort = 9091;
    private RestTemplate template = null;

    public PrometheusApiEntry(String host, int port) {
        this.host = host;
        this.port = port;
        setUp();
    }

    public void setUp() {
        HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
        httpRequestFactory.setConnectTimeout(1000);
        httpRequestFactory.setReadTimeout(2000);
        HttpClient httpClient = HttpClientBuilder.create()
                .setMaxConnTotal(100)
                .setMaxConnPerRoute(10)
                .build();
        httpRequestFactory.setHttpClient(httpClient);

        this.template = new RestTemplate(httpRequestFactory);
    }

    public String getTargetServer() {
        return String.format("http://%s:%d", this.host, this.port);
    }

    public String getPushGatewayServer() {
        return String.format("http://%s:%d", this.host, this.pushGatewayPort);
    }

    public String getPushGatewaySocket() {
        return String.format("%s:%d", this.host, this.pushGatewayPort);
    }

    public String executeGetRequest(URI url) {
        try {
            log.info("Prometheus 执行查询, url:{}", URLDecoder.decode(url.toString(), StandardCharsets.UTF_8.toString()));
        } catch (Exception e) {
            log.error("解析 URL 字符串失败, url: {} e:", url, e);
        }
        return this.template.getForObject(url, String.class);
    }

    public String executePostRequest(URI url) {
        return this.template.postForObject(url, null, String.class);
    }

    public void executeDeleteRequest(URI url) {
        this.template.delete(url);
    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public boolean disConnect() {
        return false;
    }

    @Override
    public int sendPointData(String tagName, List<DBVal> dbVals) {
        return 0;
    }

    @Override
    public int sendSinglePoint(String tagName, List<DBVal> dbVals) {
        return 0;
    }

    @Override
    public PerformanceEntity sendMultiPoint(List<DBVal> dbVals) {
        return null;
    }

    @Override
    public int updatePointValues(String tag, String startTime, String endTime, List<DBVal> updateDatas) throws ParseException {
        return 0;
    }

    @Override
    public boolean deletePointData(String tagValue) {
        return false;
    }

    @Override
    public DBVal getRTValue(DBValParam dbValParam) {
        return null;
    }

    @Override
    public DBVal getLastValue(DBValParam dbValParam) {
        return null;
    }

    @Override
    public List<DBVal> getLastValueList(List<DBValParam> dbValParams) {
        return null;
    }

    @Override
    public List<DBVal> getRTValueList(List<DBValParam> dbValParams) {
        return null;
    }

    @Override
    public List<DBVal> getRTValueListUseSplit(List<DBValParam> dbValParams) {
        return null;
    }

    @Override
    public Map<String, List<DBVal>> getHistMultiTagValsFast(List<DBValParam> dbValParams, long start, long end, int step) {
        return null;
    }

    @Override
    public List<DBVal> getHistSnap(DBValParam dbVal, long start, long end, long step) {
        return null;
    }

    @Override
    public List<DBVal> getHistSnap(DBValParam dbValParam, long startTime, long endTime, long period, long lookBack) {
        return null;
    }

    @Override
    public List<DBVal> getHistRaw(DBValParam dbVal, long tStart, long tEnd) {
        return null;
    }

    @Override
    public List<DBVal> getHistRaw(List<DBValParam> dbVals, long tStart, long tEnd) {
        return null;
    }

    @Override
    public List<DBVal> getHistInstantRaw(List<DBValParam> dbVals, long time) {
        return null;
    }

    @Override
    public DBVal getRTMinValue(DBValParam dbValParam, long startTime, long endTime) {
        return null;
    }

    @Override
    public DBVal getRTMaxValue(DBValParam dbValParam, long startTime, long endTime) {
        return null;
    }

    @Override
    public DBVal getRTAvgValue(DBValParam dbValParam, long startTime, long endTime) {
        return null;
    }

    @Override
    public AggCountResult getRTCountValue(DBValParam dbValParam, long startTime, long endTime) {
        return null;
    }

    @Override
    public List<DBVal> downSamplingQuery(AggFunctionType aggFunctionType, long timeGranularity, DBValParam dbValParam, long startTime, long endTime) {
        return null;
    }
}
