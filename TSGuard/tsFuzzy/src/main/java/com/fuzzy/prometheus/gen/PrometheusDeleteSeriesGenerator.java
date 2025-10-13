package com.fuzzy.prometheus.gen;

import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.prometheus.PrometheusGlobalState;
import com.fuzzy.prometheus.apiEntry.PrometheusDeleteParam;
import com.fuzzy.prometheus.apiEntry.PrometheusRequestType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrometheusDeleteSeriesGenerator {
    private final PrometheusGlobalState globalState;

    public PrometheusDeleteSeriesGenerator(PrometheusGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(PrometheusGlobalState globalState) {
        return new PrometheusDeleteSeriesGenerator(globalState).create();
    }

    private SQLQueryAdapter create() {
        // 删除 series 初始化值
        ExpectedErrors errors = new ExpectedErrors();
        // 首个 point 值前的所有数据均为无效数据, 剔除
        long startTime = globalState.getOptions().getStartTimestampOfTSData() - 3600 * 1000;
        long endTime = globalState.getOptions().getStartTimestampOfTSData() - 5 * 1000;
        PrometheusDeleteParam deleteParam = new PrometheusDeleteParam(globalState.getDatabaseName(), startTime, endTime);
        log.info("删除表初始化点, database name: {} startTime:{} endTime:{}", globalState.getDatabaseName(), startTime, endTime);
        return new SQLQueryAdapter(deleteParam.genPrometheusRequestParam(PrometheusRequestType.SERIES_DELETE),
                errors, false);
    }

}
