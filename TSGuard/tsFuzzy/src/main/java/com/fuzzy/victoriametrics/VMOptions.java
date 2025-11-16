package com.fuzzy.victoriametrics;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fuzzy.DBMSSpecificOptions;
import com.fuzzy.OracleFactory;
import com.fuzzy.common.oracle.TestOracle;
import com.fuzzy.victoriametrics.VMOptions.VMOracleFactory;
import com.fuzzy.victoriametrics.oracle.VMStreamComputingOracle;

import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "VM (default port: " + VMOptions.DEFAULT_PORT
        + ", default host: " + VMOptions.DEFAULT_HOST + ")")
public class VMOptions implements DBMSSpecificOptions<VMOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8428;

    @Parameter(names = "--oracle")
    public List<VMOracleFactory> oracles = Arrays.asList(VMOracleFactory.StreamComputing);

    public enum VMOracleFactory implements OracleFactory<VMGlobalState> {
        StreamComputing {
            @Override
            public TestOracle<VMGlobalState> create(VMGlobalState globalState) throws Exception {
                return new VMStreamComputingOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        };
    }

    @Override
    public List<VMOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
