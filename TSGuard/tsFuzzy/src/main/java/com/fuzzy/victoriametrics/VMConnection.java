package com.fuzzy.victoriametrics;

import com.benchmark.commonClass.TSFuzzyConnection;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.victoriametrics.apientry.VMApiEntry;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.sql.SQLException;

@Slf4j
public class VMConnection implements TSFuzzyConnection {

    private VMApiEntry apiEntry;

    public VMConnection(String host, int port) {
        this.apiEntry = new VMApiEntry(host, port);
    }

    @Override
    public VMStatement createStatement() throws SQLException {
        return new VMStatement(apiEntry);
    }

    @Override
    public TSFuzzyStatement prepareStatement(String sql) throws SQLException {
        log.warn("prepareStatement is not ready.");
        return null;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    // 测试联通性
    public boolean isClosed() throws SQLException {
        URI url = URI.create(this.apiEntry.getTargetServer() + "/-/healthy");
        String result = this.apiEntry.executeGetRequest(url);
        return !result.toLowerCase().contains("VictoriaMetrics is Healthy.".toLowerCase());
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        return "version 1.129.1";
    }
}
