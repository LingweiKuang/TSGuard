package com.fuzzy.common.tsaf.samplingfrequency;

import com.fuzzy.Randomly;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SamplingFrequencyManager {
    private volatile static SamplingFrequencyManager instance;
    // database_table -> SamplingFrequency
    private final Map<String, SamplingFrequency> samplingFrequencyHashMap = new HashMap<>();

    public static SamplingFrequencyManager getInstance() {
        if (instance == null) {
            synchronized (SamplingFrequencyManager.class) {
                if (instance == null)
                    instance = new SamplingFrequencyManager();
            }
        }
        return instance;
    }

    public void deleteSamplingFrequencyFromDatabase(String databaseName) {
        this.samplingFrequencyHashMap.entrySet().removeIf(entry -> !entry.getKey().startsWith(databaseName));
    }

    public void addSamplingFrequency(String databaseName, String tableName, Long startTimestamp, Long samplingPeriod,
                                     Long samplingNumber) {
        SamplingFrequency samplingFrequency = new SamplingFrequency(Randomly.smallNumber(), startTimestamp,
                samplingPeriod, samplingNumber);
        this.samplingFrequencyHashMap.put(genHashKey(databaseName, tableName), samplingFrequency);
    }

    public void addSamplingFrequency(String databaseName, String tableName, Long startTimestamp, Long samplingPeriod,
                                     Long samplingNumber, SamplingFrequency.SamplingFrequencyType type) {
        SamplingFrequency samplingFrequency = new SamplingFrequency(Randomly.smallNumber(), startTimestamp,
                samplingPeriod, samplingNumber, type);
        this.samplingFrequencyHashMap.put(genHashKey(databaseName, tableName), samplingFrequency);
    }

    public SamplingFrequency getSamplingFrequencyFromCollection(String databaseName, String tableName) {
        if (!this.samplingFrequencyHashMap.containsKey(genHashKey(databaseName, tableName))) {
            throw new AssertionError();
        }
        return this.samplingFrequencyHashMap.get(genHashKey(databaseName, tableName));
    }

    public List<SamplingFrequency> getAllSamplingFrequencyFromCollection(String databaseName) {
        return this.samplingFrequencyHashMap.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(databaseName))
                .map(Map.Entry::getValue).toList();
    }

    public String genHashKey(String databaseName, String tableName) {
        return String.format("%s_%s", databaseName, tableName);
    }
}
