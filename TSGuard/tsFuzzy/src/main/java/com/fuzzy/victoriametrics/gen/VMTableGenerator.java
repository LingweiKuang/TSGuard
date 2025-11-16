package com.fuzzy.victoriametrics.gen;

import com.alibaba.fastjson.JSONObject;
import com.fuzzy.Randomly;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.victoriametrics.VMGlobalState;
import com.fuzzy.victoriametrics.VMSchema;
import com.fuzzy.victoriametrics.apientry.VMRequestParam;
import com.fuzzy.victoriametrics.apientry.VMRequestType;

import java.util.ArrayList;
import java.util.List;

public class VMTableGenerator {

    public static final long SAMPLING_NUMBER = 10;
    private final String tableName;
    private final Randomly r;
    private final List<String> columns = new ArrayList<>();
    private final VMSchema schema;
    private final VMGlobalState globalState;

    public VMTableGenerator(VMGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.r = globalState.getRandomly();
        this.schema = globalState.getSchema();
        this.globalState = globalState;

        // SamplingFrequency Setting, 即本轮数据库测试所横跨的全部时间范围及采样间隔
        // 每个周期采样数目: SAMPLING_NUMBER, 每个点采样频率均匀分布理应: globalState.getOptions().getSamplingFrequency()
        SamplingFrequencyManager.getInstance().addSamplingFrequency(globalState.getDatabaseName(),
                tableName, globalState.getOptions().getStartTimestampOfTSData(),
                SAMPLING_NUMBER * globalState.getOptions().getSamplingFrequency(), SAMPLING_NUMBER);
    }

    public static SQLQueryAdapter generate(VMGlobalState globalState, String tableName) {
        return new VMTableGenerator(globalState, tableName).create();
    }

    private SQLQueryAdapter create() {
        // 创建Table -> 创建若干column
        ExpectedErrors errors = new ExpectedErrors();

        // 采取 InfluxDB Line Protocol 方式插入数据
        String databaseName = this.globalState.getDatabaseName();
        StringBuilder lineProtocolData = new StringBuilder();
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            String columnName = DBMSCommon.createColumnName(i);

            lineProtocolData.append(databaseName)
                    .append(",")
                    .append(TimeSeriesLabelConstant.TABLE.getLabel()).append("=").append(tableName)
                    .append(",")
                    .append(TimeSeriesLabelConstant.TIME_SERIES.getLabel()).append("=").append(columnName)
                    .append(" ")
                    .append("value=0")
                    .append(" ")
                    .append(System.currentTimeMillis() - 360L * 24 * 3600 * 1000)
                    .append("\n");
        }

        String query = JSONObject.toJSONString(new VMRequestParam(
                VMRequestType.INSERT_DATA, lineProtocolData.substring(0, lineProtocolData.length() - 1)));
        return new SQLQueryAdapter(query, errors, true);
    }

}
