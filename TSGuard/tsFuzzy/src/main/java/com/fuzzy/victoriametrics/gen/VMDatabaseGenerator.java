package com.fuzzy.victoriametrics.gen;

import com.alibaba.fastjson.JSONObject;
import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.victoriametrics.VMGlobalState;
import com.fuzzy.victoriametrics.apientry.VMRequestParam;
import com.fuzzy.victoriametrics.apientry.VMRequestType;

public class VMDatabaseGenerator {
    private final String databaseName;
    private final Randomly r;
    private final VMGlobalState globalState;

    public VMDatabaseGenerator(VMGlobalState globalState, String databaseName) {
        this.databaseName = databaseName;
        this.r = globalState.getRandomly();
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(VMGlobalState globalState, String databaseName) {
        return new VMDatabaseGenerator(globalState, databaseName).create();
    }


    private SQLQueryAdapter create() {
        ExpectedErrors errors = new ExpectedErrors();

        // 采取 InfluxDB Line Protocol 方式插入数据
        StringBuilder lineProtocolData = new StringBuilder();
        lineProtocolData.append(databaseName)
                .append(",")
                .append(TimeSeriesLabelConstant.TABLE.getLabel())
                .append("=")
                .append(TimeSeriesLabelConstant.DATABASE_INIT.getLabel())
                .append(" ")
                .append("value=0")
                .append(" ")
                .append(System.currentTimeMillis() - 360L * 24 * 3600 * 1000);
        String query = JSONObject.toJSONString(new VMRequestParam(
                VMRequestType.INSERT_DATA, lineProtocolData.toString()));
        return new SQLQueryAdapter(query, errors, true);
    }

}
