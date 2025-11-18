package com.fuzzy.common.tsaf.samplingfrequency;

import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.tsaf.ConstraintValue;
import com.fuzzy.common.tsaf.ConstraintValueGenerator;
import com.fuzzy.common.tsaf.RangeConstraint;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;

import java.math.BigDecimal;
import java.util.*;

public class SamplingFrequency {
    private final int seed;
    // 全局最初始时间戳
    private final Long startTimestamp;
    // 采样周期时间长度, 单位ms
    private final Long samplingPeriod;
    // 每个采样周期内采样数目
    private final Long samplingNumber;
    private final SamplingFrequencyType type;

    public SamplingFrequency(int seed, Long startTimestamp, Long samplingPeriod, Long samplingNumber) {
        this.seed = seed;
        this.startTimestamp = startTimestamp;
        this.samplingPeriod = samplingPeriod;
        this.samplingNumber = samplingNumber;
//        this.type = Randomly.fromOptions(SamplingFrequencyType.values());
        this.type = SamplingFrequencyType.NORMAL_DISTRIBUTION;
    }

    public SamplingFrequency(int seed, Long startTimestamp, Long samplingPeriod, Long samplingNumber,
                             SamplingFrequencyType samplingFrequencyType) {
        this.seed = seed;
        this.startTimestamp = startTimestamp;
        this.samplingPeriod = samplingPeriod;
        this.samplingNumber = samplingNumber;
        this.type = samplingFrequencyType;
    }

    public enum SamplingFrequencyType {
        UNIFORM_DISTRIBUTION {
            @Override
            public List<Long> apply(Random random, long startTimestamp, long endTimestamp, long samplingPeriod,
                                    long samplingNumber) {
                List<Long> timestamps = new ArrayList<>();
                assert (endTimestamp - startTimestamp) % samplingNumber == 0;
                long interval = (endTimestamp - startTimestamp) / samplingNumber;
                for (long timestamp = startTimestamp; timestamp < endTimestamp; timestamp += interval) {
                    timestamps.add(timestamp);
                }
                return timestamps;
            }

            @Override
            public List<Long> applyClosedInterval(Random random, long startTimestamp, long endTimestamp,
                                                  long samplingPeriod, long samplingNumber) {
                if (endTimestamp == startTimestamp) return Collections.singletonList(endTimestamp);

                List<Long> timestamps = new ArrayList<>();
                long interval = samplingPeriod / samplingNumber;
                for (long timestamp = startTimestamp; timestamp <= endTimestamp; timestamp += interval) {
                    timestamps.add(timestamp);
                }
                return timestamps;
            }
        },
        NORMAL_DISTRIBUTION {
            @Override
            public List<Long> apply(Random random, long startTimestamp, long endTimestamp, long samplingPeriod,
                                    long samplingNumber) {
                // 默认最小采样 1s一个点
                long minSamplingF = Long.parseLong("1" +
                        String.valueOf(samplingPeriod / samplingNumber).substring(1));
                // mean 和 stdDev 需要和时间范围值(endTimestamp 和 startTimestamp)解耦
                long min = 1;
                long max = samplingPeriod / minSamplingF;
                double mean = (double) (max + min) / 2;
                double stdDev = ((double) max - min) / 6;

                Set<Long> uniqueNumbers = new HashSet<>();
                for (int i = 0; i < samplingNumber; i++) {
                    double value = mean + stdDev * random.nextGaussian();
                    int intValue = (int) Math.round(value);
                    long timestamp = startTimestamp + (intValue - 1) * minSamplingF;
                    if (intValue >= min && intValue <= max && timestamp <= endTimestamp)
                        uniqueNumbers.add(timestamp);
                }
                return new ArrayList<>(uniqueNumbers);
            }

            @Override
            public List<Long> applyClosedInterval(Random random, long startTimestamp, long endTimestamp,
                                                  long samplingPeriod, long samplingNumber) {
                if (startTimestamp == endTimestamp) {
                    return new ArrayList<>();
                }
                return apply(random, startTimestamp, endTimestamp, samplingPeriod, samplingNumber);
            }
        };

        public abstract List<Long> apply(Random random, long startTimestamp, long endTimestamp, long samplingPeriod,
                                         long samplingNumber);

        public abstract List<Long> applyClosedInterval(Random random, long startTimestamp, long endTimestamp, long samplingPeriod,
                                                       long samplingNumber);
    }

    // 左闭右开
    public List<Long> apply(long startTimestamp, long endTimestamp) {
        List<Long> results = new ArrayList<>();
        assert (endTimestamp - startTimestamp) % samplingPeriod == 0;
        for (Long i = startTimestamp; i < endTimestamp; i += samplingPeriod) {
            results.addAll(type.apply(new Random(seed), i, i + samplingPeriod,
                    samplingPeriod, samplingNumber));
        }
        Collections.sort(results);
        return results;
    }

    // 左闭右闭
    public List<Long> applyClosedInterval(long startTimestamp, long endTimestamp) {
        List<Long> results = new ArrayList<>();
        for (long i = startTimestamp; i <= endTimestamp; i += samplingPeriod) {
            results.addAll(type.applyClosedInterval(new Random(seed), i, Math.min(i + samplingPeriod, endTimestamp),
                    samplingPeriod, samplingNumber));
        }
        Collections.sort(results);
        return results;
    }

    /**
     * 找到采样范围内最左侧采样时间戳 => 非均匀分布情况, 用于 Prometheus
     *
     * @param startTimestamp
     * @param endTimestamp
     * @return
     */
    public Long genStartSamplingTimestamp(long startTimestamp, long endTimestamp) {
        assert (endTimestamp - startTimestamp) % samplingPeriod == 0;
        return type.apply(new Random(seed), startTimestamp, startTimestamp + samplingPeriod,
                samplingPeriod, samplingNumber).stream().min(Long::compare).orElse(startTimestamp);
    }

    public BigDecimal getSeqByTimestamp(long timestamp) {
        // 取值范围 [1, ∞)
        BigDecimal seq = binarySearchTimestamp(timestamp);
        if (seq.compareTo(BigDecimal.ZERO) != 0) return seq;
        // TODO Prometheus
//        assert timestamp == 1641024000000L || timestamp == 1641024000000000L || timestamp == 1641024000000000000L;
        return BigDecimal.ONE;
    }

    private BigDecimal binarySearchTimestamp(long timestamp) {
        int round = 0;
        long sTimestamp = startTimestamp;
        long eTimestamp = startTimestamp + samplingPeriod;
        while (eTimestamp <= timestamp) {
            round++;
            sTimestamp = eTimestamp;
            eTimestamp += samplingPeriod;
        }
        BigDecimal seq = BigDecimal.valueOf(round * samplingNumber);
        List<Long> timestamps = apply(sTimestamp, eTimestamp);
        int left = 0, right = timestamps.size() - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (timestamps.get(mid) == timestamp) return seq.add(BigDecimal.valueOf(mid + 1));
            else if (timestamps.get(mid) < timestamp) left = mid + 1;
            else right = mid - 1;
        }
        return BigDecimal.ZERO;
    }

    public ConstraintValue.TimeSeriesConstraintValue transformToBaseSeqConstraintValue(
            ConstraintValue.TimeSeriesConstraintValue constraintValue) {
        TimeSeriesConstraint constraint = new TimeSeriesConstraint(GlobalConstant.BASE_TIME_SERIES_NAME);

        // 不等值
        List<BigDecimal> notEqualSeqValues = new ArrayList<>();
        constraintValue.getTimeSeriesConstraint().getNotEqualValues().forEach(
                notEqualTimestamp -> notEqualSeqValues.add(binarySearchTimestamp(notEqualTimestamp.longValue())));
        constraint.setNotEqualValues(notEqualSeqValues);

        // 不等范围
        List<RangeConstraint> rangeConstraintList = new ArrayList<>();
        for (RangeConstraint rangeConstraint : constraintValue.getTimeSeriesConstraint().getRangeConstraints()) {
            BigDecimal greaterEqualValue = findFirstGE(rangeConstraint.getGreaterEqualValue().longValue());
            BigDecimal lessEqualValue = findFirstLE(rangeConstraint.getLessEqualValue().longValue());
            rangeConstraintList.add(new RangeConstraint(greaterEqualValue, lessEqualValue));
        }
        constraint.setRangeConstraints(rangeConstraintList);
        constraint.merge();
        return new ConstraintValue.TimeSeriesConstraintValue(ConstraintValueGenerator.genBaseTimeSeries(), constraint);
    }

    private BigDecimal findFirstLE(long timestamp) {
        int round = 0;
        long sTimestamp = startTimestamp;
        long eTimestamp = startTimestamp + samplingPeriod;
        while (eTimestamp <= timestamp) {
            round++;
            sTimestamp = eTimestamp;
            eTimestamp += samplingPeriod;
            // 尾值范围最大不超过 round * samplingNumber = 30000数据点
            if (round >= 1000) return BigDecimal.valueOf(round * samplingNumber);
        }
        BigDecimal seq = BigDecimal.valueOf(round * samplingNumber);
        List<Long> timestamps = apply(sTimestamp, eTimestamp);
        int left = 0, right = timestamps.size() - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;

            if (timestamps.get(mid).compareTo(timestamp) <= 0) left = mid + 1;
            else right = mid - 1;
        }
        return seq.add(BigDecimal.valueOf(right + 1));
    }

    private BigDecimal findFirstGE(long timestamp) {
        int round = 0;
        long sTimestamp = startTimestamp;
        long eTimestamp = startTimestamp + samplingPeriod;
        while (eTimestamp <= timestamp) {
            round++;
            sTimestamp = eTimestamp;
            eTimestamp += samplingPeriod;
        }
        BigDecimal seq = BigDecimal.valueOf(round * samplingNumber);
        List<Long> timestamps = apply(sTimestamp, eTimestamp);
        int left = 0, right = timestamps.size() - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;

            if (timestamps.get(mid).compareTo(timestamp) >= 0) right = mid - 1;
            else left = mid + 1;
        }
        return seq.add(BigDecimal.valueOf(left + 1));
    }
}
