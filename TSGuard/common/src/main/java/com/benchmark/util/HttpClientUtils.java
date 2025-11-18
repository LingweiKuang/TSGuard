package com.benchmark.util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.pool.PoolStats;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HttpClientUtils {

    private static final PoolingHttpClientConnectionManager poolingConnManager = new PoolingHttpClientConnectionManager();

    static {
        poolingConnManager.setMaxTotal(20);  // 设置最大连接数
        poolingConnManager.setDefaultMaxPerRoute(5);  // 每个路由的最大连接数
        poolingConnManager.closeExpiredConnections();
        poolingConnManager.closeIdleConnections(5, TimeUnit.SECONDS);
    }

    public static String sendRequest(String url, String data, Map<String, String> heads, HttpRequestEnum requestType) {
//        log.info("execute request: url:{} data:{} requestType:{}", url, data, requestType);
//        outPoolingHttpClientConnectionState();
        switch (requestType) {
            case GET:
                return get(url, heads);
            case POST:
                return post(url, data, heads);
            case POST_WITH_URLENCODED:
                return postWithUrlEncoded(url, data, heads);
            case DELETE:
                return delete(url, heads);
            case PUT:
                return put(url, data, heads);
            default:
                log.error("发送请求类型不存在");
                return null;
        }
    }

    private static void outPoolingHttpClientConnectionState() {
        // 获取连接池的统计信息
        PoolStats totalStats = poolingConnManager.getTotalStats();
        log.info("当前连接池总连接数: " + totalStats.getMax());
        log.info("当前活动(Leased)连接数: " + totalStats.getLeased());
        log.info("当前空闲(Available)连接数: " + totalStats.getAvailable());
        log.info("当前等待(Pending)连接数: " + totalStats.getPending());
    }

    /**
     * http get
     *
     * @param url   可带参数的 url 链接
     * @param heads http 头信息
     */
    private static String get(String url, Map<String, String> heads) {
        HttpResponse httpResponse = null;
        String result = "";
        HttpGet httpGet = new HttpGet(url);
        if (heads != null) {
            Set<String> keySet = heads.keySet();
            for (String s : keySet) {
                httpGet.addHeader(s, heads.get(s));
            }
        }

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(poolingConnManager)
                .build();
        try {
            httpResponse = httpClient.execute(httpGet);
            HttpEntity httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                result = EntityUtils.toString(httpEntity, "utf-8");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * http post
     */
    private static String post(String url, String data, Map<String, String> heads) {
        HttpResponse httpResponse = null;
        String result = "";

        HttpPost httpPost = new HttpPost(url);
        if (heads != null) {
            Set<String> keySet = heads.keySet();
            for (String s : keySet) {
                httpPost.addHeader(s, heads.get(s));
            }
        }

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(poolingConnManager)
                .build();
        try {
            StringEntity s = new StringEntity(data, "utf-8");
            httpPost.setEntity(s);
            httpResponse = httpClient.execute(httpPost);
            HttpEntity httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                result = EntityUtils.toString(httpEntity, "utf-8");
            }
        } catch (IOException e) {
            log.error("发送POST请求失败, e:", e);
        }
        return result;
    }

    private static String postWithUrlEncoded(String url, String data, Map<String, String> heads) {
        HttpPost post = new HttpPost(url);

        // 设置表单格式
        post.setHeader("Content-Type", "application/x-www-form-urlencoded");
        if (heads != null) {
            Set<String> keySet = heads.keySet();
            for (String s : keySet) {
                if ("Content-Type".equalsIgnoreCase(s)) continue;
                post.addHeader(s, heads.get(s));
            }
        }

        // 构造表单内容
        List<BasicNameValuePair> params = new ArrayList<>();
        JSONObject jsonObject = JSONObject.parseObject(data);
        for (String key : jsonObject.keySet()) {
            params.add(new BasicNameValuePair(key, String.valueOf(jsonObject.get(key))));
        }

        String result = "";

        CloseableHttpClient client = HttpClients.custom()
                .setConnectionManager(poolingConnManager)
                .build();
        try {
            post.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

            // 发送请求
            HttpResponse response = client.execute(post);
            if (response.getEntity() != null) {
                result = EntityUtils.toString(response.getEntity());
            }
        } catch (Exception e) {
            log.error("发送POST请求失败, e:", e);
        }

        return result;
    }

    public static String post(String url, byte[] data, Map<String, String> headers) {
        // 创建HttpClient实例
        String result = "";

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(poolingConnManager)
                .build();
        try {
            // 创建POST请求对象
            HttpPost httpPost = new HttpPost(url);

            // 包装二进制数据为请求实体
            ByteArrayEntity entity = new ByteArrayEntity(data);

            // 设置关键请求头（根据实际需求调整）
            httpPost.setEntity(entity);

            // 可选：添加其他自定义Header
            httpPost.setHeader("Authorization", "Bearer your_token_here");
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpPost.setHeader(entry.getKey(), entry.getValue());
            }

            // 执行请求并获取响应
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                // 处理响应状态码
                int statusCode = response.getStatusLine().getStatusCode();

                // 处理响应内容
                if (statusCode >= 200 && statusCode < 300) {
                    HttpEntity responseEntity = response.getEntity();
                    result = String.format("status: %s", statusCode);
                    if (responseEntity != null)
                        result += EntityUtils.toString(responseEntity);
                } else {
                    result = String.format("Request failed with status code: %s\n res:%s", statusCode, response);
                }
            }
        } catch (Exception e) {
            log.error("发送POST请求失败, e:", e);
        }
        return result;
    }

    public static boolean sendPointDataToDB(String url, byte[] data, Map<String, String> headers) {
        try {
            URL realUrl = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) realUrl.openConnection();
            headers.forEach(conn::setRequestProperty);
            conn.setRequestProperty("accept", "*/*");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // fill and send content
            try (DataOutputStream dos = new DataOutputStream(conn.getOutputStream())) {
                dos.write(data);
                dos.flush();
            }

            // 获取返回码
            int responseCode = conn.getResponseCode();
            String responseMessage = conn.getResponseMessage();
            if (responseCode < 200 || responseCode >= 300) {
                log.error("远程写入值异常, HTTP Response: {} {}", responseCode, responseMessage);
            }

            // 读取响应内容（无论是否 2xx）
            InputStream responseStream =
                    responseCode >= 200 && responseCode < 300
                            ? conn.getInputStream()
                            : conn.getErrorStream();

            if (responseStream != null) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(responseStream))) {
                    StringBuilder responseBody = new StringBuilder();
                    String line;
                    while ((line = in.readLine()) != null) {
                        responseBody.append(line);
                    }
                    if (responseCode < 200 || responseCode >= 300) {
                        log.info("远程写入值异常, Response body: {}", responseBody);
                    }
                }
            }

            return responseCode >= 200 && responseCode < 300;
        } catch (Exception e) {
            log.error("发送错误, e:", e);
            return false;
        }
    }

    /**
     * http delete
     *
     * @param url
     * @param heads http 头信息
     */
    private static String delete(String url, Map<String, String> heads) {
        HttpResponse httpResponse = null;
        String result = "";

        HttpDelete httpDelete = new HttpDelete(url);
        if (heads != null) {
            Set<String> keySet = heads.keySet();
            for (String s : keySet) {
                httpDelete.addHeader(s, heads.get(s));
            }
        }

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(poolingConnManager)
                .build();
        try {
            httpResponse = httpClient.execute(httpDelete);
            HttpEntity httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                result = EntityUtils.toString(httpEntity, "utf-8");

            }

        } catch (IOException e) {
            e.printStackTrace();

        }
        return result;
    }

    /**
     * http put
     */
    private static String put(String url, String data, Map<String, String> heads) {
        HttpResponse httpResponse = null;
        String result = "";

        HttpPut httpPut = new HttpPut(url);
        if (heads != null) {
            Set<String> keySet = heads.keySet();
            for (String s : keySet) {
                httpPut.addHeader(s, heads.get(s));
            }
        }

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(poolingConnManager)
                .build();
        try {
            StringEntity s = new StringEntity(data, "utf-8");
            httpPut.setEntity(s);
            httpResponse = httpClient.execute(httpPut);
            HttpEntity httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                result = EntityUtils.toString(httpEntity, "utf-8");

            }

        } catch (IOException e) {
            e.printStackTrace();

        }
        return result;
    }
}
