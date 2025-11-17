package com.tsFuzzy.tsdbms.prometheus;

import com.fuzzy.common.streamprocessing.entity.TimeSeriesElement;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.TSAFDataType;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.prometheus.PrometheusVisitor;
import com.fuzzy.prometheus.ast.PrometheusExpression;
import com.fuzzy.prometheus.gen.PrometheusInsertGenerator;
import com.fuzzy.prometheus.gen.PrometheusTableGenerator;
import com.fuzzy.prometheus.streamcomputing.PrometheusTimeSeriesVector;
import com.fuzzy.prometheus.streamcomputing.parser.PrometheusLexer;
import com.fuzzy.prometheus.streamcomputing.parser.PrometheusParser;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;

public class TestPrometheusStreamComputing {

    /**
     * Arithmetic binary operator
     */
    @Test
    public void testScalarArithmeticOpScalar() {
        TimeSeriesStream.TimeSeriesScalar scalar = new TimeSeriesStream.TimeSeriesScalar(BigDecimal.ONE);
        TimeSeriesStream.TimeSeriesScalar scalar2 = new TimeSeriesStream.TimeSeriesScalar(BigDecimal.TEN);
        TimeSeriesStream.TimeSeriesScalar scalarZero = new TimeSeriesStream.TimeSeriesScalar(BigDecimal.ZERO);

        assert ((TimeSeriesStream.TimeSeriesScalar) scalar.add(scalar2)).getScalarValue().compareTo(BigDecimal.valueOf(11)) == 0;
        assert ((TimeSeriesStream.TimeSeriesScalar) scalar.subtract(scalar2)).getScalarValue().compareTo(BigDecimal.valueOf(-9)) == 0;
        assert ((TimeSeriesStream.TimeSeriesScalar) scalar.multiply(scalar2)).getScalarValue().compareTo(BigDecimal.TEN) == 0;
        assert ((TimeSeriesStream.TimeSeriesScalar) scalar.divide(scalar2)).getScalarValue().compareTo(BigDecimal.valueOf(0.1)) == 0;
        assert ((TimeSeriesStream.TimeSeriesScalar) scalar.divide(scalarZero)).getScalarValue().compareTo(TimeSeriesStream.INF_BIGDECIMAL) == 0;
    }

    @Test
    public void testScalarArithmeticOpVector() {
        TimeSeriesStream.TimeSeriesScalar scalar = new TimeSeriesStream.TimeSeriesScalar(BigDecimal.ONE);
        Long timestamp = 1L;
        Long timestamp2 = 2L;
        String hashKey = "";
        TimeSeriesElement element = new TimeSeriesElement(hashKey, new HashMap<>(), new HashMap<>() {{
            this.put(timestamp, BigDecimal.TEN);
            this.put(timestamp2, BigDecimal.ZERO);
        }});
        TimeSeriesStream.TimeSeriesVector vector = new TimeSeriesStream.TimeSeriesVector(new HashMap<>() {{
            this.put(element.getLabelSetsHashKey(), element);
        }});

        assert ((TimeSeriesStream.TimeSeriesVector) scalar.add(vector)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.valueOf(11)) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) scalar.subtract(vector)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.valueOf(-9)) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) scalar.multiply(vector)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.TEN) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) scalar.divide(vector)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.valueOf(0.1)) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) scalar.divide(vector)).getElements().get(hashKey).getValues().get(timestamp2).compareTo(TimeSeriesStream.INF_BIGDECIMAL) == 0;
    }

    @Test
    public void testVectorArithmeticOpVector() {
        Long timestamp = 1L;
        Long timestamp2 = 2L;
        String hashKey = "";

        TimeSeriesElement element = new TimeSeriesElement(hashKey, new HashMap<>(), new HashMap<>() {{
            this.put(timestamp, BigDecimal.ONE);
            this.put(timestamp2, BigDecimal.ONE);
        }});
        TimeSeriesStream.TimeSeriesVector vector = new TimeSeriesStream.TimeSeriesVector(new HashMap<>() {{
            this.put(element.getLabelSetsHashKey(), element);
        }});

        TimeSeriesElement element2 = new TimeSeriesElement(hashKey, new HashMap<>(), new HashMap<>() {{
            this.put(timestamp, BigDecimal.TEN);
            this.put(timestamp2, BigDecimal.ZERO);
        }});
        TimeSeriesStream.TimeSeriesVector vector2 = new TimeSeriesStream.TimeSeriesVector(new HashMap<>() {{
            this.put(element2.getLabelSetsHashKey(), element2);
        }});

        assert ((TimeSeriesStream.TimeSeriesVector) vector.add(vector2)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.valueOf(11)) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) vector.subtract(vector2)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.valueOf(-9)) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) vector.multiply(vector2)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.TEN) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) vector.divide(vector2)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.valueOf(0.1)) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) vector.divide(vector2)).getElements().get(hashKey).getValues().get(timestamp2).compareTo(TimeSeriesStream.INF_BIGDECIMAL) == 0;
    }

    @Test
    public void testVectorArithmeticOpScalar() {
        Long timestamp = 1L;
        String hashKey = "";

        TimeSeriesElement element = new TimeSeriesElement(hashKey, new HashMap<>(), new HashMap<>() {{
            this.put(timestamp, BigDecimal.ONE);
        }});
        TimeSeriesStream.TimeSeriesVector vector = new TimeSeriesStream.TimeSeriesVector(new HashMap<>() {{
            this.put(element.getLabelSetsHashKey(), element);
        }});
        TimeSeriesStream.TimeSeriesScalar scalar = new TimeSeriesStream.TimeSeriesScalar(BigDecimal.ZERO);

        assert ((TimeSeriesStream.TimeSeriesVector) vector.add(scalar)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.ONE) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) vector.subtract(scalar)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.ONE) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) vector.multiply(scalar)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.ZERO) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) vector.divide(scalar)).getElements().get(hashKey).getValues().get(timestamp).compareTo(TimeSeriesStream.INF_BIGDECIMAL) == 0;
    }

    /**
     * Comparison binary operator
     */
    @Test
    public void testScalarComparisonOpScalar() {
        TimeSeriesStream.TimeSeriesScalar scalar = new TimeSeriesStream.TimeSeriesScalar(BigDecimal.ONE);
        TimeSeriesStream.TimeSeriesScalar scalar2 = new TimeSeriesStream.TimeSeriesScalar(BigDecimal.TEN);

        assert ((TimeSeriesStream.TimeSeriesScalar) scalar.equal(scalar2)).getScalarValue().compareTo(BigDecimal.ZERO) == 0;
        assert ((TimeSeriesStream.TimeSeriesScalar) scalar.notEqual(scalar2)).getScalarValue().compareTo(BigDecimal.ONE) == 0;
        assert ((TimeSeriesStream.TimeSeriesScalar) scalar.greaterThan(scalar2)).getScalarValue().compareTo(BigDecimal.ZERO) == 0;
        assert ((TimeSeriesStream.TimeSeriesScalar) scalar.greaterOrEqual(scalar2)).getScalarValue().compareTo(BigDecimal.ZERO) == 0;
        assert ((TimeSeriesStream.TimeSeriesScalar) scalar.lessThan(scalar2)).getScalarValue().compareTo(BigDecimal.ONE) == 0;
        assert ((TimeSeriesStream.TimeSeriesScalar) scalar.lessOrEqual(scalar2)).getScalarValue().compareTo(BigDecimal.ONE) == 0;
    }

    @Test
    public void testScalarComparisonOpVector() {
        TimeSeriesStream.TimeSeriesScalar scalar = new TimeSeriesStream.TimeSeriesScalar(BigDecimal.ONE);
        Long timestamp = 1L;
        String hashKey = "";
        TimeSeriesElement element = new TimeSeriesElement(hashKey, new HashMap<>(), new HashMap<>() {{
            this.put(timestamp, BigDecimal.TEN);
        }});
        TimeSeriesStream.TimeSeriesVector vector = new TimeSeriesStream.TimeSeriesVector(new HashMap<>() {{
            this.put(element.getLabelSetsHashKey(), element);
        }});

        assert ((TimeSeriesStream.TimeSeriesVector) scalar.equal(vector)).getElements().isEmpty();
        assert ((TimeSeriesStream.TimeSeriesVector) scalar.notEqual(vector)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.TEN) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) scalar.greaterThan(vector)).getElements().isEmpty();
        assert ((TimeSeriesStream.TimeSeriesVector) scalar.greaterOrEqual(vector)).getElements().isEmpty();
        assert ((TimeSeriesStream.TimeSeriesVector) scalar.lessThan(vector)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.TEN) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) scalar.lessOrEqual(vector)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.TEN) == 0;
    }

    @Test
    public void testVectorComparisonOpVector() {
        Long timestamp = 1L;
        String hashKey = "";

        TimeSeriesElement element = new TimeSeriesElement(hashKey, new HashMap<>(), new HashMap<>() {{
            this.put(timestamp, BigDecimal.ONE);
        }});
        TimeSeriesStream.TimeSeriesVector vector = new TimeSeriesStream.TimeSeriesVector(new HashMap<>() {{
            this.put(element.getLabelSetsHashKey(), element);
        }});

        TimeSeriesElement element2 = new TimeSeriesElement(hashKey, new HashMap<>(), new HashMap<>() {{
            this.put(timestamp, BigDecimal.TEN);
        }});
        TimeSeriesStream.TimeSeriesVector vector2 = new TimeSeriesStream.TimeSeriesVector(new HashMap<>() {{
            this.put(element2.getLabelSetsHashKey(), element2);
        }});

        assert ((TimeSeriesStream.TimeSeriesVector) vector.equal(vector2)).getElements().isEmpty();
        assert ((TimeSeriesStream.TimeSeriesVector) vector.notEqual(vector2)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.ONE) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) vector.greaterThan(vector2)).getElements().isEmpty();
        assert ((TimeSeriesStream.TimeSeriesVector) vector.greaterOrEqual(vector2)).getElements().isEmpty();
        assert ((TimeSeriesStream.TimeSeriesVector) vector.lessThan(vector2)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.ONE) == 0;
        assert ((TimeSeriesStream.TimeSeriesVector) vector.lessOrEqual(vector2)).getElements().get(hashKey).getValues().get(timestamp).compareTo(BigDecimal.ONE) == 0;
    }

    @Test
    public void testVectorComparisonOpDouble() {
        Long timestamp = 1L;
        String hashKey = "";

        TimeSeriesElement element = new TimeSeriesElement(hashKey, new HashMap<>(), new HashMap<>() {{
            this.put(timestamp, BigDecimal.ONE);
        }});
        TimeSeriesStream.TimeSeriesVector vector = new TimeSeriesStream.TimeSeriesVector(new HashMap<>() {{
            this.put(element.getLabelSetsHashKey(), element);
        }});

        TimeSeriesStream stream = new PrometheusTimeSeriesVector(vector);
        PrometheusTimeSeriesVector trans = (PrometheusTimeSeriesVector) stream;
        System.out.println(trans);
        System.out.println(trans.isVector());
    }

    @Test
    public void testVectorComparisonOpDoubleVector() {
        BigDecimal a = new BigDecimal("0");
        BigDecimal b = new BigDecimal("-100");
        System.out.println(a.divide(b));
        System.out.println(Double.NEGATIVE_INFINITY + Double.NEGATIVE_INFINITY);
        System.out.println(Double.NEGATIVE_INFINITY == Double.NEGATIVE_INFINITY);
        System.out.println(Double.isNaN(Double.NaN));
    }

    @Test
    public void restorePromQLSolving() {
        String databaseName = "tsafdb_16_cc2768b4_e23f_456e_b6de_da749755b498";
        String tableName = "t1";
        String tableName2 = "t2";
        String columnName = "c0_counter";
        long startTimestamp = 1760423968000L;
        long samplingPointNum = 200;
        long endTimestamp = startTimestamp + samplingPointNum * 5 * 1000;
        PrometheusInsertGenerator.putLastTimestamp(databaseName, tableName, endTimestamp);
        PrometheusInsertGenerator.putLastTimestamp(databaseName, tableName2, endTimestamp);
        // ((((tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"}) / (tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"})) OR ((+ (tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"})))) * (((-677) atan2 (tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"})) OR ((tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"}) + (tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"})))) / ((((-197) - (tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"})) * ((tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"}) - (tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"}))) UNLESS (((tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"}) OR (tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"})) != ((tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"}) AND (tsafdb_23_39fdf0b3_4f65_41c0_b059_a5026968a10c{table="t1", timeSeries="c0_counter"}))))
        String expr = "(+ (tsafdb_16_cc2768b4_e23f_456e_b6de_da749755b498{table=\"t1\", timeSeries=\"c0_counter\"}))";

        // 词法分析
        List<PrometheusLexer.Token> toks = PrometheusLexer.tokenize(expr);
        System.out.println("Tokens:");
        for (PrometheusLexer.Token tk : toks) System.out.print(tk + " ");
        System.out.println("\n\nParsing...");

        // 语法解析
        PrometheusParser p = new PrometheusParser(toks);
        PrometheusExpression expression = p.parseExpression();

        // 采样点设置 => 指定采样方程, table1 and table2
        SamplingFrequencyManager.getInstance().addSamplingFrequency(databaseName,
                tableName, startTimestamp, PrometheusTableGenerator.SAMPLING_NUMBER * 5 * 1000,
                PrometheusTableGenerator.SAMPLING_NUMBER, SamplingFrequency.SamplingFrequencyType.UNIFORM_DISTRIBUTION);
        SamplingFrequencyManager.getInstance().addSamplingFrequency(databaseName,
                tableName2, startTimestamp, PrometheusTableGenerator.SAMPLING_NUMBER * 5 * 1000,
                PrometheusTableGenerator.SAMPLING_NUMBER, SamplingFrequency.SamplingFrequencyType.UNIFORM_DISTRIBUTION);

        // 数值方程组设定, table1 and table2
        EquationsManager.getInstance()
                .initEquationsFromTimeSeries(databaseName, tableName, columnName, TSAFDataType.INT);
        EquationsManager.getInstance()
                .initEquationsFromTimeSeries(databaseName, tableName2, columnName, TSAFDataType.INT);

        TimeSeriesStream timeSeriesStream = PrometheusVisitor.streamComputeTimeSeriesVector(databaseName,
                null, startTimestamp, expression, null);
        System.out.println(timeSeriesStream);
//        TimeSeriesStream.TimeSeriesVector vector = (TimeSeriesStream.TimeSeriesVector) timeSeriesStream;
//        System.out.println(vector.getElements().get("_table_t1_timeSeries_c0").getValues().get(1760420229));
    }

    @Test
    public void bigDecimalTest() {
//        0.00817995910020449897750511247443762781186094069530 actualValue:0.0081799591002045
//        -1.5565115842074999097377485668403096497058868408203 actualValue:-1.5565115842075
//        75.11799410029498525073746312684365781710914454277340000000000000000 actualValue:75.117994100295
//        -0.00518600337041149947672544420549037078252205481896 actualValue:-0.0051860033704115
        BigDecimal expectedValue = new BigDecimal("-0");
        BigDecimal actualValue = new BigDecimal("0");
        System.out.println(expectedValue.equals(actualValue));
        System.out.println(expectedValue.compareTo(actualValue));
    }
}
