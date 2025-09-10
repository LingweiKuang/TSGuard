package com.fuzzy.prometheus.feedback;

import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.feedback.QuerySynthesisFeedback;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PrometheusQuerySynthesisFeedbackManager {
    public static final Integer MAX_EXECUTION_COUNT = 10;
    public static final Integer MAX_REGENERATE_COUNT_PER_ROUND = 10;

    public static AtomicInteger expressionDepth = new AtomicInteger(0);
    public static final QuerySynthesisFeedback querySynthesisFeedback = new QuerySynthesisFeedback();
    public static final QueryExecutionStatistical queryExecutionStatistical = new QueryExecutionStatistical();
    private static final Set<String> reportedBugSequence = new HashSet<>();

    static {

    }

    public static void addSequenceRegenerateProbability(String sequence) {
        querySynthesisFeedback.addSequenceRegenerateProbability(sequence);
    }

    public static void setSequenceRegenerateProbabilityToMax(String sequence) {
        querySynthesisFeedback.setSequenceRegenerateProbabilityToMax(sequence, MAX_EXECUTION_COUNT);
    }

    public static boolean isRegenerateSequence(String sequence) {
        // 已报告错误序列 -> 需要重新生成
        for (String sequenceNeedToRegenerate : reportedBugSequence)
            if (sequence.contains(sequenceNeedToRegenerate)) return true;
        return querySynthesisFeedback.isRegenerateSequence(sequence, MAX_EXECUTION_COUNT);
    }

    public static void incrementExpressionDepth(int maxExpressionDepth) {
        if (expressionDepth.get() < maxExpressionDepth) {
            int depth = expressionDepth.incrementAndGet();
            log.info("递增迭代表达式深度, 当前深度:{}", depth);
        }
    }

    public static long incrementSuccessQueryCount() {
        return queryExecutionStatistical.incrementSuccessQueryCount();
    }

    public static long incrementErrorQueryCount() {
        return queryExecutionStatistical.incrementErrorQueryCount();
    }

    public static long incrementInvalidQueryCount() {
        return queryExecutionStatistical.incrementInvalidQueryCount();
    }
}
