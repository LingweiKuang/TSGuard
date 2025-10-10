package com.fuzzy.prometheus;


import com.alibaba.fastjson.JSONObject;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.schema.*;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.prometheus.apiEntry.PrometheusQueryParam;
import com.fuzzy.prometheus.apiEntry.PrometheusRequestType;
import com.fuzzy.prometheus.apiEntry.entity.PrometheusSeriesResultItem;
import com.fuzzy.prometheus.ast.PrometheusConstant;
import com.fuzzy.prometheus.resultSet.PrometheusResultSet;

import java.sql.SQLException;
import java.util.*;

public class PrometheusSchema extends AbstractSchema<PrometheusGlobalState, PrometheusSchema.PrometheusTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    @Override
    public boolean containsTableWithZeroRows(PrometheusGlobalState globalState) {
        // Prometheus 默认插入一行数据, 故检测空值改为1
        return getDatabaseTables().stream().anyMatch(t -> t.getNrRows(globalState) == 1);
    }

    public enum CommonDataType {
        INT, DOUBLE, BOOLEAN, NULL, BIGDECIMAL;

        public static CommonDataType getRandom(PrometheusGlobalState globalState) {
            if (globalState.usesPQS() || globalState.usesTSAF() || globalState.usesStreamComputing()) {
                return Randomly.fromOptions(CommonDataType.INT, CommonDataType.DOUBLE);
            } else {
                return Randomly.fromOptions(values());
            }
        }
    }

    public enum PrometheusDataType {
        COUNTER, GAUGE, HISTOGRAM, SUMMARY;

        public static PrometheusSchema.PrometheusDataType[] valuesPQS() {
            return new PrometheusSchema.PrometheusDataType[]{GAUGE};
        }

        public static PrometheusSchema.PrometheusDataType[] valuesTSAFOrStreamComputing() {
            return new PrometheusSchema.PrometheusDataType[]{GAUGE};
        }

        public static PrometheusDataType getRandom(PrometheusGlobalState globalState) {
            if (globalState.usesPQS() || globalState.usesTSAF() || globalState.usesStreamComputing()) {
                // TODO
//                return Randomly.fromOptions(PrometheusDataType.COUNTER, PrometheusDataType.GAUGE);
                return PrometheusDataType.GAUGE;
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

    public static class PrometheusColumn extends AbstractTableColumn<PrometheusTable, PrometheusDataType> {

        public PrometheusColumn(String name, boolean isTag, PrometheusDataType type) {
            super(name, null, type, isTag);
        }

    }

    public static class PrometheusTables extends AbstractTables<PrometheusTable, PrometheusColumn> {

        public PrometheusTables(List<PrometheusTable> tables) {
            super(tables);
        }

        public PrometheusRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            return null;
        }

    }

    public static class PrometheusRowValue extends AbstractRowValue<PrometheusTables, PrometheusColumn, PrometheusConstant> {

        PrometheusRowValue(PrometheusTables tables, Map<PrometheusColumn, PrometheusConstant> values) {
            super(tables, values);
        }

    }

    public static class PrometheusTable extends AbstractRelationalTable<PrometheusColumn, PrometheusIndex, PrometheusGlobalState> {

        public PrometheusTable(String name, String databaseName, List<PrometheusColumn> columns, List<PrometheusIndex> indexes) {
            super(name, databaseName, columns, indexes, false);
        }

        @Override
        public String selectCountStatement() {
            String queryBody = String.format("count_over_time(%s{table='%s'}[1h])", this.getDatabaseName(),
                    this.getSelectCountTableName());
            return new PrometheusQueryParam(queryBody).genPrometheusRequestParam(PrometheusRequestType.INSTANT_QUERY);
        }

    }

    public static final class PrometheusIndex extends TableIndex {

        protected PrometheusIndex(String indexName) {
            super(indexName);
        }

    }

    public static PrometheusSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        // 查询表结构 -> show series
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<PrometheusTable> databaseTables = new ArrayList<>();
                try (TSFuzzyStatement s = con.createStatement()) {
                    String databaseMatch = String.format("match[]={__name__=\"%s\"}", databaseName);
                    // 查询数据库结构
                    try (PrometheusResultSet prometheusResultSet = (PrometheusResultSet)
                            s.executeQuery(new PrometheusQueryParam(databaseMatch)
                                    .genPrometheusRequestParam(PrometheusRequestType.SERIES_QUERY))) {

                        // 将获取数据按照 <database, table, timeSeries> 去重
                        Set<PrometheusSeriesResultItem> seriesSet = new HashSet<>();
                        while (prometheusResultSet.hasNext()) {
                            Object rowRecord = prometheusResultSet.getCurrentValue();
                            PrometheusSeriesResultItem seriesResultItem =
                                    JSONObject.parseObject(rowRecord.toString(), PrometheusSeriesResultItem.class);
                            // DatabaseInit 不纳入数据库表数量计算
                            if (seriesResultItem.getTable().equals(TimeSeriesLabelConstant.DATABASE_INIT.getLabel()))
                                continue;

                            if (seriesSet.contains(seriesResultItem)) continue;
                            else seriesSet.add(seriesResultItem);
                        }

                        // tableName -> columns
                        Map<String, List<PrometheusColumn>> tableToSeriesMap = new HashMap<>();
                        seriesSet.forEach(item -> {
                            if (tableToSeriesMap.containsKey(item.getTable())) {
                                tableToSeriesMap.get(item.getTable()).add(item.transToColumn());
                            } else {
                                List<PrometheusColumn> items = new ArrayList<>();
                                items.add(item.transToColumn());
                                tableToSeriesMap.put(item.getTable(), items);
                            }
                        });

                        // table -> columns
                        tableToSeriesMap.entrySet().forEach(entry -> {
                            PrometheusTable t =
                                    new PrometheusTable(entry.getKey(), databaseName, entry.getValue(), null);
                            entry.getValue().forEach(column -> column.setTable(t));
                            databaseTables.add(t);
                        });
                    }
                }
                return new PrometheusSchema(databaseTables);
            } catch (Exception e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    public PrometheusSchema(List<PrometheusTable> databaseTables) {
        super(databaseTables);
    }

    public PrometheusTables getRandomTableNonEmptyTables() {
        return new PrometheusTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

}
