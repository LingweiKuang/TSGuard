package com.tsFuzzy.tsdbms.vm;

import com.fuzzy.Main;
import com.fuzzy.common.constant.GlobalConstant;
import com.tsFuzzy.tsdbms.TestConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestVM {

    @Test
    public void testStreamComputing() {
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--host", "localhost", "--port", "8428", "--precision", "ms",
                        "--log-syntax-error-query", "true", "--max-expression-depth", "4",
                        "--log-execution-time", "false", "--num-tries", "150",
                        "--drop-database",
                        "--params", "", "--start-timestamp", "1735660800",
                        "--use-syntax-validator", "--use-syntax-sequence",
                        "--random-string-generation", "ALPHANUMERIC", "--database-prefix",
                        "tsafdb", "--max-generated-databases", "1",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.VICTORIA_METRICS_DATABASE_NAME, "--oracle", "StreamComputing"}));
    }
}
