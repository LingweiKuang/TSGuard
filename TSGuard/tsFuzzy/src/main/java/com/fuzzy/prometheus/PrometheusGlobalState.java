package com.fuzzy.prometheus;

import com.fuzzy.SQLGlobalState;
import com.fuzzy.prometheus.PrometheusOptions.PrometheusOracleFactory;

import java.sql.SQLException;

public class PrometheusGlobalState extends SQLGlobalState<PrometheusOptions, PrometheusSchema> {

    @Override
    protected PrometheusSchema readSchema() throws SQLException {
        return PrometheusSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == PrometheusOracleFactory.PQS);
    }

    public boolean usesTSAF() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == PrometheusOracleFactory.TSAF);
    }

    public boolean usesStreamComputing() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == PrometheusOracleFactory.StreamComputing);
    }
}
