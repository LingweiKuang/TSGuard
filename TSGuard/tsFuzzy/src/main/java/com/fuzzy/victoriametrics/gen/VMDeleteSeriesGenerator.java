package com.fuzzy.victoriametrics.gen;

import com.alibaba.fastjson.JSONObject;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.victoriametrics.VMGlobalState;
import com.fuzzy.victoriametrics.VMSchema;
import com.fuzzy.victoriametrics.apientry.VMRequestParam;
import com.fuzzy.victoriametrics.apientry.VMRequestType;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class VMDeleteSeriesGenerator {
    private final VMGlobalState globalState;

    public VMDeleteSeriesGenerator(VMGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(VMGlobalState globalState) {
        return new VMDeleteSeriesGenerator(globalState).create();
    }

    private SQLQueryAdapter create() {
        // 删除一切不在 globalState 内的 series
        List<VMSchema.VMTable> databaseTables = globalState.getSchema().getDatabaseTables();
        String timeSeriesNames = databaseTables.stream().flatMap(table -> table.getColumns().stream())
                .map(VMSchema.VMColumn::getName)
                .collect(Collectors.joining("|"));
        String tableNames = databaseTables.stream().map(VMSchema.VMTable::getName)
                .collect(Collectors.joining("|"));

        JSONObject requestBody = new JSONObject();
        // match[]={__name__="metric_value"}
        requestBody.put("match[]", String.format("{__name__=\"%s\",%s!~\"%s\",%s!~\"%s\"}",
                globalState.getDatabaseName() + TimeSeriesLabelConstant.END_WITH_VALUE.getLabel(),
                TimeSeriesLabelConstant.TABLE.getLabel(), tableNames,
                TimeSeriesLabelConstant.TIME_SERIES.getLabel(), timeSeriesNames));
        VMRequestParam requestParam = new VMRequestParam(VMRequestType.SERIES_DELETE, requestBody.toJSONString());
        log.info("删除表初始化点, database name: {}", globalState.getDatabaseName());
        return new SQLQueryAdapter(JSONObject.toJSONString(requestParam), new ExpectedErrors(), false);
    }

}
