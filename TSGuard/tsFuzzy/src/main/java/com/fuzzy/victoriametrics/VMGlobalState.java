package com.fuzzy.victoriametrics;

import com.fuzzy.SQLGlobalState;

import java.sql.SQLException;

public class VMGlobalState extends SQLGlobalState<VMOptions, VMSchema> {

    @Override
    protected VMSchema readSchema() throws SQLException {
        return VMSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesStreamComputing() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == VMOptions.VMOracleFactory.StreamComputing);
    }
}
