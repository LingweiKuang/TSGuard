package com.tsFuzzy.tsdbms.prometheus;

import com.fuzzy.common.streamprocessing.entity.TimeSeriesElement;
import com.fuzzy.common.streamprocessing.entity.TimeSeriesStream;
import com.fuzzy.prometheus.streamcomputing.PrometheusTimeSeriesVector;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.HashMap;

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
        BigDecimal a = new BigDecimal("1180976132.000000000000000");
        BigDecimal b = new BigDecimal("1180976132");
        MathContext mc = new MathContext(12); // 保留 12 位有效数字
        BigDecimal aRounded = a.round(mc);
        BigDecimal bRounded = b.round(mc);
        System.out.println(aRounded);
        System.out.println(bRounded);
        System.out.println(aRounded.compareTo(bRounded) == 0);

        BigDecimal c = new BigDecimal("84");
        BigDecimal d = new BigDecimal("-1721638059");
        // -4.879074295603731E-8
        System.out.println(c.divide(d, 25, RoundingMode.HALF_UP));
    }
}
