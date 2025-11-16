package com.fuzzy.victoriametrics.resultset;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.benchmark.entity.DBValResultSet;
import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesElement;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.victoriametrics.apientry.VMRequestType;
import lombok.Data;
import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class VMResultSet extends DBValResultSet {

    private Object jsonResult;
    private Object curRowRecord;
    private VMRequestType vmRequestType;

    public VMResultSet(String db, String table, String jsonResult) {
        super(db, table);
        JSONObject jsonObject = JSONObject.parseObject(jsonResult);
        this.jsonResult = jsonObject.get("data");
    }

    public VMResultSet(String jsonResult, VMRequestType vmRequestType) {
        JSONObject jsonObject = JSONObject.parseObject(jsonResult);
        this.jsonResult = jsonObject.get("data");
        this.vmRequestType = vmRequestType;
    }

    @Override
    protected boolean hasValue() throws SQLException {
        if (jsonResult instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) jsonResult;
            return !jsonArray.isEmpty() && cursor < jsonArray.size();
        } else {
            return !ObjectUtil.isNull(jsonResult) && cursor < 1;
        }
    }

    @Override
    public boolean hasNext() throws SQLException {
        try {
            cursor++;
            boolean hasValue = hasValue();
            if (!hasValue) return false;
            if (jsonResult instanceof JSONArray) {
                this.curRowRecord = ((JSONArray) jsonResult).get(cursor);
            } else {
                this.curRowRecord = jsonResult;
            }
            return true;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException {
//        try {
//            this.jsonResult.close();
//        } catch (Exception e) {
//            throw new SQLException(e.getMessage());
//        }
    }

    public String get(int rowIndex) throws SQLException {
//        try {
//            while (jsonResult.hasNext()) {
//
//            }
//            return null;
//        } catch (Exception e) {
//            throw new SQLException(e.getMessage());
//        }
        return null;
    }

    public Object getCurrentValue() throws SQLException {
        if (curRowRecord != null) return curRowRecord;
        else throw new SQLException("curRowRecord is null!");
    }

    /**
     * @param columnLabel
     * @return int
     * @description 查找除时间戳外的Column
     * @dateTime 2024/4/24 15:57
     */
    @Override
    public int findColumn(String columnLabel) throws SQLException {
//        try {
//            List<String> columnNames = jsonResult.getColumnNames();
//            for (int i = 0; i < columnNames.size(); i++) {
//                if (columnNames.get(i).equalsIgnoreCase(columnLabel)) return i - 1;
//            }
//            return -1;
//        } catch (Exception e) {
//            throw new SQLException(e.getMessage());
//        }
        return 0;
    }

    /**
     * 解析 JSON 为 TimeSeriesStream
     *
     * @return
     * @throws SQLException
     */
    public TimeSeriesStream genTimeSeriesStream() throws SQLException {
        // 将整个结果集转换为 时间戳 -> 结果列表 的组合
        TimeSeriesStream.TimeSeriesVector timeSeriesVector = new TimeSeriesStream.TimeSeriesVector();
        // 查询结果为空
        JSONArray resultArray = ((JSONObject) jsonResult).getJSONArray("result");
        if (ObjectUtils.isEmpty(resultArray)) return timeSeriesVector;

        // 遍历每条时间序列, 进行 element 赋值
        for (int i = 0; i < resultArray.size(); i++) {
            JSONObject row = (JSONObject) resultArray.get(i);
            JSONArray values = null;
            switch (this.vmRequestType) {
                case INSTANT_QUERY:
                    values = row.getJSONArray("value");
                    break;
                case RANGE_QUERY:
                    values = row.getJSONArray("values");
                    break;
                default:
                    throw new SQLException(String.format("VMRequestType not support! val:%s", vmRequestType));
            }

            // timestamp and value
            Map<Long, BigDecimal> timestampToValues = new HashMap<>();
            for (int j = 0; j < values.size(); j++) {
                Long timestamp = Long.parseLong(values.getJSONArray(j).getString(0));
                String strValue = values.getJSONArray(j).getString(1);
                BigDecimal value;
                value = switch (strValue) {
                    case "+Inf" -> TimeSeriesStream.INF_BIGDECIMAL;
                    case "-Inf" -> TimeSeriesStream.NEGATE_INF_BIGDECIMAL;
                    case "NaN" -> TimeSeriesStream.NAN_BIGDECIMAL;
                    default -> new BigDecimal(strValue);
                };
                timestampToValues.put(timestamp, value);
            }

            // label sets
            JSONObject metricObject = row.getJSONObject("metric");
            String metricName = metricObject.getString(TimeSeriesLabelConstant.METRIC_KEY.getLabel());
            metricName = metricName == null ? "" : metricName;
            HashMap<String, String> labelSets = new HashMap<>() {{
                put(TimeSeriesLabelConstant.TABLE.getLabel(), metricObject.getString(TimeSeriesLabelConstant.TABLE.getLabel()));
                put(TimeSeriesLabelConstant.TIME_SERIES.getLabel(),
                        metricObject.getString(TimeSeriesLabelConstant.TIME_SERIES.getLabel()));
            }};

            // element
            TimeSeriesElement timeSeriesElement = new TimeSeriesElement(metricName, labelSets, timestampToValues);
            // OR 运算符产生多条 metrics 时间序列的情况 => 一个时间戳上具备多个 val, 忽略 metric name, 将集合合并比较
            if (timeSeriesVector.getElements().containsKey(timeSeriesElement.getLabelSetsHashKey())) {
                timeSeriesVector.getElements().get(timeSeriesElement.getLabelSetsHashKey()).getValues().putAll(timestampToValues);
            } else {
                timeSeriesVector.getElements().put(timeSeriesElement.getLabelSetsHashKey(), timeSeriesElement);
            }

        }

        return timeSeriesVector;
    }

    public Map<Long, List<BigDecimal>> genTimestampToValuesMap() throws SQLException {
        // 将整个结果集转换为 时间戳 -> 结果列表 的组合
        Map<Long, List<BigDecimal>> results = new HashMap<>();
        // 查询结果为空
        JSONArray resultArray = ((JSONObject) jsonResult).getJSONArray("result");
        if (ObjectUtils.isEmpty(resultArray)) return results;

        JSONObject result = ((JSONObject) jsonResult).getJSONArray("result").getJSONObject(0);
        JSONArray values = null;
        switch (this.vmRequestType) {
            case INSTANT_QUERY:
                values = result.getJSONArray("value");
                break;
            case RANGE_QUERY:
                values = result.getJSONArray("values");
                break;
            default:
                throw new SQLException(String.format("VMRequestType not support! val:%s", vmRequestType));
        }
        for (int i = 0; i < values.size(); i++) {
            Long timestamp = Long.parseLong(values.getJSONArray(i).getString(0));
            BigDecimal value = new BigDecimal(values.getJSONArray(i).getString(1));
            if (results.containsKey(timestamp)) {
                results.get(timestamp).add(value);
            } else {
                List<BigDecimal> bigDecimalList = Collections.singletonList(value);
                results.put(timestamp, bigDecimalList);
            }
        }
        return results;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        // TODO 暂仅支持捞取第一批数据 -> 即精确查询
        JSONObject result = ((JSONObject) jsonResult).getJSONArray("result").getJSONObject(0);
        JSONArray values = null;
        switch (this.vmRequestType) {
            case INSTANT_QUERY:
                values = result.getJSONArray("value");
                return values.getString(columnIndex);
            case RANGE_QUERY:
                values = result.getJSONArray("values");
                return values.getJSONArray(columnIndex).getString(0);
            default:
                throw new SQLException(String.format("VMRequestType not support! val:%s", vmRequestType));
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return Integer.valueOf(getString(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return Long.parseLong(getString(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return Double.parseDouble(getString(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return new BigDecimal(getString(columnIndex));
    }

//    public Timestamp getTimestamp() throws SQLException {
//        return new Timestamp(getCurrentValue().getTimestamp());
//    }

}
