package com.fuzzy.iotdb.gen;

import com.fuzzy.Randomly;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.iotdb.IotDBGlobalState;

public class IotDBTableGenerator {
    public static final long SAMPLING_NUMBER = 30;
    private final StringBuilder sb = new StringBuilder();
    private final String tableName;
    private final IotDBGlobalState globalState;

    public IotDBTableGenerator(IotDBGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.globalState = globalState;
        SamplingFrequencyManager.getInstance().addSamplingFrequency(globalState.getDatabaseName(), tableName,
                globalState.getOptions().getStartTimestampOfTSData(),
                SAMPLING_NUMBER * globalState.getOptions().getSamplingFrequency(), SAMPLING_NUMBER);
        SamplingFrequencyManager.getInstance().addSamplingFrequency(globalState.getDatabaseName(),
                "aligned_" + tableName, globalState.getOptions().getStartTimestampOfTSData(),
                SAMPLING_NUMBER * globalState.getOptions().getSamplingFrequency(), SAMPLING_NUMBER);
    }

    public static SQLQueryAdapter generate(IotDBGlobalState globalState, String tableName) {
        return new IotDBTableGenerator(globalState, tableName).create();
    }

    private SQLQueryAdapter create() {
        // 创建时间序列数据, 创建table
        int columnNumber = Randomly.smallNumber();
        for (int i = 0; i < columnNumber; i++) {
            SQLQueryAdapter columnCreateStatement = IotDBColumnGenerator.generate(
                    globalState, tableName, "c" + i);
            try {
                globalState.executeStatement(columnCreateStatement);
            } catch (Exception e) {

            }
        }

        // 保证该表最少具备一列
        SQLQueryAdapter columnCreateStatement = IotDBColumnGenerator.generate(globalState, tableName,
                "c" + columnNumber);
        return new SQLQueryAdapter(columnCreateStatement.getQueryString(), columnCreateStatement.getExpectedErrors(),
                true);
    }
}
