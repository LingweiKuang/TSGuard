package com.tsFuzzy.tsdbms.prometheus;

import com.fuzzy.Main;
import com.fuzzy.common.constant.GlobalConstant;
import com.tsFuzzy.tsdbms.TestConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPrometheus {

    @Test
    public void testPQS() {
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--random-string-generation", "ALPHANUMERIC",
                        "--host", "111.229.183.22", "--port", "9990",
                        "--database-prefix", "pqsdb", "--max-generated-databases", "1",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.IOTDB_DATABASE_NAME, "--oracle", "PQS"}));
    }

    @Test
    public void testTSAF() {
        // 测试数据时间范围：[cur - 3400, cur], unit: s
        // TODO startTimestamp 需要随着测试的进行同步修正
        long curTimestamp = System.currentTimeMillis() / 1000;
        long startTimestamp = curTimestamp - 3000;

        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--host", "localhost", "--port", "9090", "--precision", "ms",
                        "--log-syntax-error-query", "true", "--max-expression-depth", "4",
                        "--log-execution-time", "false", "--num-tries", "2000",
                        "--start-timestamp", String.valueOf(startTimestamp),
//                        "--drop-database",
                        "--params", "",
                        "--use-syntax-validator", "--use-syntax-sequence",
                        "--random-string-generation", "ALPHANUMERIC", "--database-prefix",
                        "tsafdb", "--max-generated-databases", "1",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.PROMETHEUS_DATABASE_NAME, "--oracle", "TSAF"}));
    }

    // curl -X POST -g 'http://127.0.0.1:9090/api/v1/admin/tsdb/delete_series?match[]=tsafdbconnectiontest'
    // curl -X POST -g 'http://127.0.0.1:9090/api/v1/admin/tsdb/delete_series?match[]={"__name__":"tsafdb_0_2cedd0e7_ca5f_44d6_a128_6f37f3fc7854"}'
}
