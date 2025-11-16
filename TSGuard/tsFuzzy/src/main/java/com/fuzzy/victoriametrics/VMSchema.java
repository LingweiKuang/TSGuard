package com.fuzzy.victoriametrics;


import com.alibaba.fastjson.JSONObject;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.schema.*;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.victoriametrics.apientry.VMRequestParam;
import com.fuzzy.victoriametrics.apientry.VMRequestType;
import com.fuzzy.victoriametrics.apientry.entity.VMSeriesResultItem;
import com.fuzzy.victoriametrics.ast.VMConstant;
import com.fuzzy.victoriametrics.resultset.VMResultSet;

import java.sql.SQLException;
import java.util.*;

public class VMSchema extends AbstractSchema<VMGlobalState, VMSchema.VMTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    @Override
    public boolean containsTableWithZeroRows(VMGlobalState globalState) {
        // VM 默认插入一行数据, 故检测空值改为1
        return getDatabaseTables().stream().anyMatch(t -> t.getNrRows(globalState) == 1);
    }

    public enum CommonDataType {
        INT, DOUBLE, BOOLEAN, NULL, BIGDECIMAL;

        public static CommonDataType getRandom(VMGlobalState globalState) {
            if (globalState.usesStreamComputing()) {
                return Randomly.fromOptions(CommonDataType.INT, CommonDataType.DOUBLE);
            } else {
                return Randomly.fromOptions(values());
            }
        }
    }

    public enum VMDataType {
        COUNTER, GAUGE, HISTOGRAM, SUMMARY;

        public static VMDataType[] valuesPQS() {
            return new VMDataType[]{GAUGE};
        }

        public static VMDataType[] valuesTSAFOrStreamComputing() {
            return new VMDataType[]{COUNTER, GAUGE};
        }

        public static VMDataType getRandom(VMGlobalState globalState) {
            if (globalState.usesStreamComputing()) {
                return Randomly.fromOptions(valuesTSAFOrStreamComputing());
            } else {
                return Randomly.fromOptions(values());
            }
        }

        public boolean isNumeric() {
            switch (this) {
                case COUNTER:
                case GAUGE:
                case HISTOGRAM:
                case SUMMARY:
                    return true;
                default:
                    throw new AssertionError(this);
            }
        }

        public boolean isInt() {
            // TODO
            return true;
//            switch (this) {
//                case COUNTER:
//                case GAUGE:
//                case HISTOGRAM:
//                case SUMMARY:
//                    return true;
//                default:
//                    throw new AssertionError(this);
//            }
        }
    }

    public static class VMColumn extends AbstractTableColumn<VMTable, VMDataType> {

        public VMColumn(String name, boolean isTag, VMDataType type) {
            super(name, null, type, isTag);
        }

    }

    public static class VMTables extends AbstractTables<VMTable, VMColumn> {

        public VMTables(List<VMTable> tables) {
            super(tables);
        }

        public VMRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            return null;
        }

    }

    public static class VMRowValue extends AbstractRowValue<VMTables, VMColumn, VMConstant> {

        VMRowValue(VMTables tables, Map<VMColumn, VMConstant> values) {
            super(tables, values);
        }

    }

    public static class VMTable extends AbstractRelationalTable<VMColumn, VMIndex, VMGlobalState> {

        public VMTable(String name, String databaseName, List<VMColumn> columns, List<VMIndex> indexes) {
            super(name, databaseName, columns, indexes, false);
        }

        @Override
        public String selectCountStatement() {
            // 查询 365d 内数据点数目
            JSONObject queryBody = new JSONObject();
            queryBody.put("query", String.format("count_over_time(%s{table='%s'})",
                    this.getDatabaseName() + TimeSeriesLabelConstant.END_WITH_VALUE.getLabel(),
                    this.getSelectCountTableName()));
            queryBody.put("step", "365d");
            return JSONObject.toJSONString(new VMRequestParam(VMRequestType.INSTANT_QUERY, queryBody.toJSONString()));
        }

    }

    public static final class VMIndex extends TableIndex {

        protected VMIndex(String indexName) {
            super(indexName);
        }

    }

    public static VMSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        // 查询表结构 -> show series
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<VMTable> databaseTables = new ArrayList<>();
                try (TSFuzzyStatement s = con.createStatement()) {
                    JSONObject body = new JSONObject();
                    // match[]={__name__="metric_value"}
                    body.put("match[]", String.format("{__name__=\"%s\"}",
                            databaseName + TimeSeriesLabelConstant.END_WITH_VALUE.getLabel()));
                    body.put("start", System.currentTimeMillis() - 365L * 24 * 3600 * 1000);
                    body.put("end", System.currentTimeMillis());
                    // 查询数据库结构
                    try (VMResultSet VMResultSet = (VMResultSet) s.executeQuery(
                            JSONObject.toJSONString(new VMRequestParam(VMRequestType.SERIES_QUERY, body.toJSONString())))) {

                        // 将获取数据按照 <database, table, timeSeries> 去重
                        Set<VMSeriesResultItem> seriesSet = new HashSet<>();
                        while (VMResultSet.hasNext()) {
                            Object rowRecord = VMResultSet.getCurrentValue();
                            VMSeriesResultItem seriesResultItem =
                                    JSONObject.parseObject(rowRecord.toString(), VMSeriesResultItem.class);
                            // DatabaseInit 不纳入数据库表数量计算
                            if (seriesResultItem.getTable().equals(TimeSeriesLabelConstant.DATABASE_INIT.getLabel()))
                                continue;

                            if (seriesSet.contains(seriesResultItem)) continue;
                            else seriesSet.add(seriesResultItem);
                        }

                        // tableName -> columns
                        Map<String, List<VMColumn>> tableToSeriesMap = new HashMap<>();
                        seriesSet.forEach(item -> {
                            if (tableToSeriesMap.containsKey(item.getTable())) {
                                tableToSeriesMap.get(item.getTable()).add(item.transToColumn());
                            } else {
                                List<VMColumn> items = new ArrayList<>();
                                items.add(item.transToColumn());
                                tableToSeriesMap.put(item.getTable(), items);
                            }
                        });

                        // table -> columns
                        tableToSeriesMap.entrySet().forEach(entry -> {
                            VMTable t =
                                    new VMTable(entry.getKey(), databaseName, entry.getValue(), null);
                            entry.getValue().forEach(column -> column.setTable(t));
                            databaseTables.add(t);
                        });
                    }
                }
                return new VMSchema(databaseTables);
            } catch (Exception e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    public VMSchema(List<VMTable> databaseTables) {
        super(databaseTables);
    }

    public VMTables getRandomTableNonEmptyTables() {
        return new VMTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

}
