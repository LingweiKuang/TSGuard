package com.tsFuzzy.tsdbms.prometheus;

import build.buf.gen.io.prometheus.write.v2.Request;
import build.buf.gen.io.prometheus.write.v2.Sample;
import build.buf.gen.io.prometheus.write.v2.TimeSeries;
import build.buf.gen.prometheus.Label;
import com.benchmark.util.HttpClientUtils;
import com.fuzzy.prometheus.PrometheusConnection;
import com.fuzzy.prometheus.PrometheusStatement;
import com.fuzzy.prometheus.apiEntry.PrometheusInsertParam;
import com.fuzzy.prometheus.apiEntry.entity.CollectorAttribute;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import io.prometheus.client.Collector;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PrometheusPushClient {

    private static final String ENDPOINT_URL = "http://localhost:9090/api/v1/write";

    public static void main(String[] args) throws IOException {
        // 创建指标数据
        Request request = createSampleRequest();

        // 序列化为Protobuf字节
        ByteString data = request.toByteString();

        // Snappy压缩
        byte[] compressed = compress(data.toByteArray());

        // 构建HTTP请求
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/x-protobuf;proto=io.prometheus.write.v2.Request");
        headers.put("Content-Encoding", "snappy");
        headers.put("X-Prometheus-Remote-Write-Version", "2.0.0");
        boolean res = HttpClientUtils.sendPointDataToDB(ENDPOINT_URL, compressed, headers);
    }

    private static Request createSampleRequest() {
        Request.Builder requestBuilder = Request.newBuilder();

        // 添加标签到符号表（严格交替存储）
        Map<String, String> labels = new HashMap<>();
        labels.put("job", "app");
        labels.put("instance", "server1");
        labels.put("__name__", "http_requests_total_test_103");
        addSymbol(requestBuilder, labels);
        List<Integer> labelRefs = IntStream.range(0, labels.size() * 2)
                .boxed()
                .collect(Collectors.toList());

        // 创建 TimeSeries
        TimeSeries.Builder tsBuilder = TimeSeries.newBuilder();
        tsBuilder.addAllLabelsRefs(labelRefs);

        List<Sample> sampleList = new ArrayList<>();
        sampleList.add(Sample.newBuilder()
                .setValue(42.0)
                .setTimestamp(1747557831000L)
                .build());
        sampleList.add(Sample.newBuilder()
                .setValue(42.0)
                .setTimestamp(System.currentTimeMillis())
                .build());
        sampleList.sort(Comparator.comparingLong(Sample::getTimestamp));

        tsBuilder.addAllSamples(sampleList);
        return requestBuilder
                .addTimeseries(tsBuilder.build())
                .build();
    }

    private static int addSymbol(Request.Builder requestBuilder, String value) {
        int index = requestBuilder.getSymbolsList().size();
        requestBuilder.addSymbols(value);
        return index;
    }

    private static void addSymbol(Request.Builder requestBuilder, Map<String, String> labels) {
        labels.forEach((label, value) -> {
            requestBuilder.addSymbols(label);
            requestBuilder.addSymbols(value);
        });
    }

    private static byte[] compress(byte[] data) throws IOException {
        return Snappy.compress(data);
    }

    @Test
    public void testPrometheusInsertParam() throws IOException, InterruptedException {
        // TODO 不同时序之间不共享时间戳进度，但是应该有阈值要求回填不能超过多少范围
        // TODO 要求回填阈值不超过当前时间 3600 s
        // TODO Prometheus 数据采样
        CollectorAttribute collectorAttribute = new CollectorAttribute();
        String metricName = "db0_uuid_4";
        collectorAttribute.setMetricName(metricName);
        collectorAttribute.setTableName("t0");
        collectorAttribute.setTimeSeriesName("ts8");
        collectorAttribute.getDoubleValues().add(40.0);
        collectorAttribute.getTimestamps().add(1749260880000L);
        collectorAttribute.getDoubleValues().add(41.0);
        collectorAttribute.getTimestamps().add(1748136858175L);

        Map<String, String> labels = new HashMap<>();
        labels.put("label", "val");
        String metricName0 = "db1_uuid0";
        CollectorAttribute collectorAttribute0 = new CollectorAttribute();
        collectorAttribute0.setMetricName(metricName0);
        collectorAttribute0.setTableName("t1");
        collectorAttribute0.setTimeSeriesName("ts8");
        collectorAttribute0.setLabels(labels);
        collectorAttribute0.getDoubleValues().add(40.0);
        collectorAttribute0.getDoubleValues().add(41.0);
        collectorAttribute0.getTimestamps().add(1748136859175L);
        collectorAttribute0.getTimestamps().add(1748136858175L);

        PrometheusInsertParam insertParam = new PrometheusInsertParam();
        insertParam.getCollectorMap().put(metricName, collectorAttribute);
        insertParam.getCollectorMap().put(metricName0, collectorAttribute0);
        byte[] compressed = insertParam.snappyCompressedRequest(metricName);
        byte[] compressed2 = insertParam.snappyCompressedRequest(metricName0);

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/x-protobuf;proto=io.prometheus.write.v2.Request");
        headers.put("Content-Encoding", "snappy");
        headers.put("X-Prometheus-Remote-Write-Version", "2.0.0");
        boolean res = HttpClientUtils.sendPointDataToDB(ENDPOINT_URL, compressed, headers);
        boolean res2 = HttpClientUtils.sendPointDataToDB(ENDPOINT_URL, compressed2, headers);
    }

    @Test
    public void testPrometheusConnection() throws Exception {
        String host = "localhost";
        int port = 9090;

        CollectorAttribute collectorAttribute = new CollectorAttribute();
        String metricName = "tsafdb_0_a12d53b5_1cdc_4301_bdac_77fea73f7629";
        // attribute
        collectorAttribute.setMetricName(metricName);
        collectorAttribute.setTableName("t0");
        collectorAttribute.setTimeSeriesName("ts8");
        collectorAttribute.getDoubleValues().add(40.0);
        collectorAttribute.getTimestamps().add(1759909216000L);
        // attribute 0
        CollectorAttribute collectorAttribute0 = new CollectorAttribute();
        collectorAttribute0.setMetricName(metricName);
        collectorAttribute0.setTableName("t1");
        collectorAttribute0.setTimeSeriesName("ts8");
        collectorAttribute0.getDoubleValues().add(40.0);
        collectorAttribute0.getTimestamps().add(1759909096000L);

        Map<String, CollectorAttribute> collectorMap = new HashMap<>();
        collectorMap.put(collectorAttribute.getUniqueHashKey(), collectorAttribute);
        collectorMap.put(collectorAttribute0.getUniqueHashKey(), collectorAttribute0);
        PrometheusInsertParam insertParam = new PrometheusInsertParam();
        insertParam.setCollectorMap(collectorMap);
        String requestParam = insertParam.genPrometheusQueryParam();

        PrometheusConnection prometheusConnection = new PrometheusConnection(host, port);
        try (PrometheusStatement s = prometheusConnection.createStatement()) {
            s.execute(requestParam);
        }
    }

    @Test
    public void testClient() throws Exception {
        // 1. 创建 MetricFamilySamples 实例
        List<Collector.MetricFamilySamples> samplesList = new ArrayList<>();

        // 示例1：Counter类型指标
        Collector.MetricFamilySamples counterMFS = new Collector.MetricFamilySamples(
                "http_requests_total",  // 指标名称
                Collector.Type.COUNTER, // 类型
                "Total HTTP requests",  // 帮助信息
                Collections.singletonList(                  // 样本数据
                        new Collector.MetricFamilySamples.Sample(
                                "test_metrics{method=\"GET\"}", // 标签
                                new ArrayList<>(),                                   // 标签值（此处为null）
                                new ArrayList<>(),
                                100.0,
                                System.currentTimeMillis()
                        )
                )
        );
        samplesList.add(counterMFS);

        // 示例2：Gauge类型指标
        Collector.MetricFamilySamples gaugeMFS = new Collector.MetricFamilySamples(
                "memory_usage_bytes",
                Collector.Type.GAUGE,
                "Current memory usage",
                Collections.singletonList(new Collector.MetricFamilySamples.Sample(
                        "test_metrics_bytes{region=\"us-east\"}",
                        new ArrayList<>(),
                        new ArrayList<>(),
                        100.0,
                        System.currentTimeMillis()
                ))
        );
        samplesList.add(gaugeMFS);

        // 2. 转换为 Enumeration
        Enumeration<Collector.MetricFamilySamples> mfsEnumeration =
                Collections.enumeration(samplesList);

        // 使用 Prometheus Java 客户端生成 OpenMetrics 数据
//        StringWriter writer = new StringWriter();
//        TextFormat.writeFormat("application/openmetrics-text; version=1.0.0; charset=utf-8", writer, mfsEnumeration);
//        String openMetricsData = writer.toString();

        // 通过 HTTP 客户端发送
        build.buf.gen.prometheus.TimeSeries timeSeries = buildTimeSeries();
        byte[] inputBytes = timeSeries.toByteArray();
        byte[] compressedData = Snappy.compress(inputBytes);
        String url = "http://172.29.185.200:9090/api/v1/write";
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/x-protobuf;proto=io.prometheus.write.v2.Request");
        headers.put("Content-Encoding", "snappy");
        headers.put("X-Prometheus-Remote-Write-Version", "2.0.0");
        headers.put("User-Agent", "MyClient/1.0 (Linux x86_64; Go 1.20)");
        boolean res = HttpClientUtils.sendPointDataToDB(url, compressedData, headers);
        System.out.println(res);
//        HttpClientUtils.sendRequest(, openMetricsData, headers,
//                HttpRequestEnum.POST);
//        HttpClient client = HttpClient.newHttpClient();
//        HttpRequest request = HttpRequest.newBuilder()
//                .uri(URI.create("http://prometheus:9090/api/v1/write"))
//                .header("Content-Type", "application/x-protobuf")
//                .POST(HttpRequest.BodyPublishers.ofString(openMetricsData))
//                .build();
//        client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public static build.buf.gen.prometheus.TimeSeries buildTimeSeries() {
        build.buf.gen.prometheus.TimeSeries.Builder builder = build.buf.gen.prometheus.TimeSeries.newBuilder();

        // 1. labels
        List<Label> labels = new ArrayList<>();
        labels.add(Label.newBuilder()
                .setName("__name__")
                .setValue("metric_test")
                .build());
        labels.add(Label.newBuilder()
                .setName("test")
                .setValue("test")
                .build());
        builder.addAllLabels(labels);

        // 2. 添加 Samples
        List<build.buf.gen.prometheus.Sample> samples = new ArrayList<>();
        samples.add(build.buf.gen.prometheus.Sample.newBuilder()
                .setValue(42.5)
                .setTimestamp(1747534061406L)
                .build());
        builder.addAllSamples(samples);
        return builder.build();
    }

    @Test
    public void testPrometheusWrite() {
        String targetUrl = "http://localhost:9090";

        HttpURLConnection connection = null;
        OutputStream httpOutputStream = null;
        CodedOutputStream codedOutputStream = null;

        try {
            // 1. 创建 HTTP 连接
            URL url = new URL(targetUrl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/x-protobuf;proto=io.prometheus.write.v2.Request"); // 设置 Content-Type
            connection.setRequestProperty("Content-Encoding", "snappy");
            connection.setRequestProperty("X-Prometheus-Remote-Write-Version", "2.0.0");
            connection.setRequestProperty("User-Agent", "MyClient/1.0 (Linux x86_64; Go 1.20)");

            // 2. 获取 HTTP 输出流并包装为 CodedOutputStream
            httpOutputStream = connection.getOutputStream();
            codedOutputStream = CodedOutputStream.newInstance(httpOutputStream);

            build.buf.gen.prometheus.TimeSeries.Builder builder = build.buf.gen.prometheus.TimeSeries.newBuilder();
            build.buf.gen.prometheus.TimeSeries timeSeries = builder.addSamples(build.buf.gen.prometheus.Sample.getDefaultInstance()).build();
            timeSeries.writeTo(codedOutputStream);


            // 4. 处理响应
            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()))) {
                String line;
                StringBuilder response = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                System.out.println("Response: " + response);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}