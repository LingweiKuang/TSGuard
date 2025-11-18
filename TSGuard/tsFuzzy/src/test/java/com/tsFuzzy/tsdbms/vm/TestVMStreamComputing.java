package com.tsFuzzy.tsdbms.vm;

import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.TSAFDataType;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.victoriametrics.VMVisitor;
import com.fuzzy.victoriametrics.ast.VMExpression;
import com.fuzzy.victoriametrics.gen.VMInsertGenerator;
import com.fuzzy.victoriametrics.gen.VMTableGenerator;
import com.fuzzy.victoriametrics.parser.VMLexer;
import com.fuzzy.victoriametrics.parser.VMParser;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestVMStreamComputing {

    @Test
    public void restorePromQLSolving() {
        // Init config
        String databaseName = "tsafdb0_value";
        String tableName = "t1";
        String tableName2 = "t2";
        String columnName = "c0";
        long startTimestamp = 1760423968000L;
        long samplingPointNum = 10;
        long endTimestamp = startTimestamp + samplingPointNum * 5 * 1000;

        VMInsertGenerator.putLastTimestamp(databaseName, tableName, endTimestamp);
        VMInsertGenerator.putLastTimestamp(databaseName, tableName2, endTimestamp);

        // 采样点设置 => 指定采样方程, table1 and table2
        SamplingFrequencyManager.getInstance().addSamplingFrequency(databaseName,
                tableName, startTimestamp, VMTableGenerator.SAMPLING_NUMBER * 5 * 1000,
                VMTableGenerator.SAMPLING_NUMBER, SamplingFrequency.SamplingFrequencyType.UNIFORM_DISTRIBUTION);
        SamplingFrequencyManager.getInstance().addSamplingFrequency(databaseName,
                tableName2, startTimestamp, VMTableGenerator.SAMPLING_NUMBER * 5 * 1000,
                VMTableGenerator.SAMPLING_NUMBER, SamplingFrequency.SamplingFrequencyType.UNIFORM_DISTRIBUTION);

        // 数值方程组设定, table1 and table2
        EquationsManager.getInstance()
                .initEquationsFromTimeSeries(databaseName, tableName, columnName, TSAFDataType.INT);
        EquationsManager.getInstance()
                .initEquationsFromTimeSeries(databaseName, tableName2, columnName, TSAFDataType.INT);

        List<String> exprs = Arrays.asList("(tsafdb2_value{table=\"t1\", timeSeries=\"c0\"}) != (tsafdb2_value{table=\"t2\", timeSeries=\"c0\"})",
                "(tsafdb2_value{table=\"t1\", timeSeries=\"c0\"}) != ((tsafdb2_value{table=\"t2\", timeSeries=\"c0\"}) > (1000))",
                "((tsafdb0_value{table=\"t1\", timeSeries=\"c0\"}) AND (tsafdb0_value{table=\"t1\", timeSeries=\"c0\"})) < ((tsafdb0_value{table=\"t1\", timeSeries=\"c0\"}) <= (-287))",
                "((tsafdb0_value{table=\"t1\", timeSeries=\"c0\"}) - (tsafdb0_value{table=\"t1\", timeSeries=\"c0\"})) != ((tsafdb0_value{table=\"t1\", timeSeries=\"c0\"}) UNLESS (tsafdb0_value{table=\"t1\", timeSeries=\"c0\"}))");

        for (String expr : exprs) {
            System.out.println("expr:" + expr);

            // 词法分析
            List<VMLexer.Token> toks = VMLexer.tokenize(expr);
            System.out.println("Tokens:");
            for (VMLexer.Token tk : toks) System.out.print(tk + " ");
            System.out.println("\n\nParsing...");

            // 语法解析
            VMParser p = new VMParser(toks);
            VMExpression expression = p.parseExpression();

            TimeSeriesStream timeSeriesStream = VMVisitor.streamComputeTimeSeriesVector(databaseName,
                    null, startTimestamp, endTimestamp, expression, null);
            System.out.println(timeSeriesStream);
            System.out.println("===================================================================");
        }
    }

    @Test
    public void restorePromQLSolvingSingle() {
        // Init config
        String databaseName = "tsafdb0_value";
        String tableName = "t1";
        String tableName2 = "t2";
        String columnName = "c0";
        String columnName2 = "c1";
        long startTimestamp = 1735660800000L;
        long endTimestamp = 1735660900000L;

        VMInsertGenerator.putLastTimestamp(databaseName, tableName, endTimestamp);
        VMInsertGenerator.putLastTimestamp(databaseName, tableName2, endTimestamp);

        // 采样点设置 => 指定采样方程, table1 and table2
        SamplingFrequencyManager.getInstance().addSamplingFrequency(databaseName,
                tableName, startTimestamp, VMTableGenerator.SAMPLING_NUMBER * 5 * 1000,
                VMTableGenerator.SAMPLING_NUMBER, SamplingFrequency.SamplingFrequencyType.UNIFORM_DISTRIBUTION);
        SamplingFrequencyManager.getInstance().addSamplingFrequency(databaseName,
                tableName2, startTimestamp, VMTableGenerator.SAMPLING_NUMBER * 5 * 1000,
                VMTableGenerator.SAMPLING_NUMBER, SamplingFrequency.SamplingFrequencyType.UNIFORM_DISTRIBUTION);

        // 数值方程组设定, table1 and table2
        EquationsManager.getInstance()
                .initEquationsFromTimeSeries(databaseName, tableName, columnName, TSAFDataType.INT);
        EquationsManager.getInstance()
                .initEquationsFromTimeSeries(databaseName, tableName, columnName2, TSAFDataType.INT);
        EquationsManager.getInstance()
                .initEquationsFromTimeSeries(databaseName, tableName2, columnName, TSAFDataType.INT);
        EquationsManager.getInstance()
                .initEquationsFromTimeSeries(databaseName, tableName2, columnName2, TSAFDataType.INT);

        String expr = "(tsafdb32_value{table=\"t1\", timeSeries=\"c0\"}) / (-0)";

        // 词法分析
        List<VMLexer.Token> toks = VMLexer.tokenize(expr);
        System.out.println("Tokens:");
        for (VMLexer.Token tk : toks) System.out.print(tk + " ");
        System.out.println("\n\nParsing...");

        // 语法解析
        VMParser p = new VMParser(toks);
        VMExpression expression = p.parseExpression();

        TimeSeriesStream timeSeriesStream = VMVisitor.streamComputeTimeSeriesVector(databaseName,
                null, startTimestamp, endTimestamp, expression, null);
        System.out.println(timeSeriesStream);
    }

    @Test
    public void test() {
        System.out.println(Math.atan2(1, Double.POSITIVE_INFINITY));
        System.out.println(Math.atan2(Double.POSITIVE_INFINITY, 1));
    }

}
