package com.fuzzy.victoriametrics.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.gen.UntypedExpressionGenerator;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.victoriametrics.VMGlobalState;
import com.fuzzy.victoriametrics.VMSchema.VMColumn;
import com.fuzzy.victoriametrics.VMSchema.VMDataType;
import com.fuzzy.victoriametrics.VMSchema.VMRowValue;
import com.fuzzy.victoriametrics.ast.*;
import com.fuzzy.victoriametrics.ast.VMBinaryComparisonOperation.BinaryComparisonOperator;
import com.fuzzy.victoriametrics.ast.VMBinaryLogicalOperation.VMBinaryLogicalOperator;
import com.fuzzy.victoriametrics.ast.VMUnaryPrefixOperation.VMUnaryPrefixOperator;
import com.fuzzy.victoriametrics.feedback.VMQuerySynthesisFeedbackManager;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class VMExpressionGenerator extends UntypedExpressionGenerator<VMExpression, VMColumn> {

    // 父节点后不允许跟子节点
    public static final Set<String> pairingProhibited = new HashSet<>();
    private final VMGlobalState state;
    private VMRowValue rowVal;

    public VMExpressionGenerator(VMGlobalState state) {
        this.state = state;
        if (state.getOptions().useSyntaxValidator()) initGenerator();
    }

    public VMExpressionGenerator setRowVal(VMRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

    private void initGenerator() {
        // TODO
        // 查询有效性（靠标记规避语法不符合的节点）
        // 经过严格配对实验，以下各种组合均属于语法错误
    }

    private String genHashKeyWithPairActions(final Actions parentNode, final Actions childNode) {
        return String.format("%s__%s", parentNode.name(), childNode.name());
    }

    private enum Actions {
        COLUMN, LITERAL, UNARY_PREFIX_OPERATION,
        BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON_OPERATION, BINARY_ARITHMETIC_OPERATION,
//        COMPUTABLE_FUNCTION(false),
        ;
    }

    @Override
    public VMExpression generateExpression(int depth) {
        return null;
    }

    @Override
    protected VMExpression generateExpression(Object parentActions, int depth) {
        try {
            if (depth >= (state.getOptions().useSyntaxSequence() ?
                    VMQuerySynthesisFeedbackManager.expressionDepth.get() :
                    state.getOptions().getMaxExpressionDepth())) {
                return generateLeafNode();
            }
            Actions actions = Randomly.fromOptions(Actions.values());
            // 语法序列组合校验
            while (!checkExpressionValidity(parentActions, actions)) {
                actions = Randomly.fromOptions(Actions.values());
            }
            return generateSpecifiedExpression(actions, parentActions, depth);
        } catch (ReGenerateExpressionException e) {
            return generateExpression(parentActions, depth);
        }
    }

    @Override
    public VMExpression generateExpressionForSyntaxValidity(String fatherActions, String childActions) {
        return null;
    }

    private VMExpression generateSpecifiedExpression(Actions actions, Object parentActions, int depth) {
        VMExpression expression;
        switch (actions) {
            case COLUMN:
                expression = generateColumn();
                break;
            case LITERAL:
                expression = generateConstant();
                break;
            case UNARY_PREFIX_OPERATION:
                expression = new VMUnaryPrefixOperation(generateExpression(actions, depth + 1),
//                        VMUnaryPrefixOperator.getRandom()
                        VMUnaryPrefixOperator.PLUS
                );
                break;
//            case COMPUTABLE_FUNCTION:
//                expression = getComputableFunction();
//                break;
            case BINARY_LOGICAL_OPERATOR:
                // set operator "and" not allowed in binary scalar expression
                VMExpression leftExpression = generateExpression(actions, depth + 1);
                while (leftExpression.isScalarExpression()) {
                    leftExpression = generateExpression(actions, depth + 1);
                }
                VMExpression rightExpression = generateExpression(actions, depth + 1);
                while (rightExpression.isScalarExpression()) {
                    rightExpression = generateExpression(actions, depth + 1);
                }

                expression = new VMBinaryLogicalOperation(leftExpression, rightExpression,
                        VMBinaryLogicalOperator.getRandom());
                break;
            case BINARY_COMPARISON_OPERATION:
                expression = new VMBinaryComparisonOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        BinaryComparisonOperator.getRandom());
                break;
            case BINARY_ARITHMETIC_OPERATION:
                expression = new VMBinaryArithmeticOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        VMBinaryArithmeticOperation.VMBinaryArithmeticOperator.getRandom());
                break;
            default:
                throw new AssertionError();
        }
        if (state.getOptions().useSyntaxValidator()) expression.checkSyntax();
        return expression;
    }

    private boolean checkExpressionValidity(final Object parentActions, final Actions childActions) {
        if (parentActions == null) return true;
        else if (pairingProhibited.contains(genHashKeyWithPairActions(Actions.valueOf(parentActions.toString()),
                childActions)))
            return false;
        return true;
    }

    private VMExpression ignoreThisExpr(Object parentActions, Actions action, int depth) {
        return generateExpression(parentActions, depth);
    }

    @Override
    public VMExpression generateConstant() {
        VMDataType[] values;
        if (state.usesStreamComputing()) {
            values = VMDataType.valuesTSAFOrStreamComputing();
        } else {
            values = VMDataType.values();
        }
        // 仅返回 -1000～1000值, 防止乘法范围溢出
        switch (Randomly.fromOptions(values)) {
            case COUNTER:
            case GAUGE:
                return VMConstant.createIntConstant(state.getRandomly().getInteger(-1000, 1000));
//            case DOUBLE:
//                double val = BigDecimal.valueOf((double) state.getRandomly().getInteger()
//                        / state.getRandomly().getInteger()).setScale(
//                        VMDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
//                return VMDoubleConstant.createDoubleConstant(val);
            default:
                throw new AssertionError();
        }
    }

    @Override
    protected VMExpression generateColumn() {
        VMColumn c = Randomly.fromList(columns);
        VMConstant val = null;
        if (rowVal == null) {
            // TSQS生成表达式时, rowVal默认值为1, 列存在因子时能够进行区分
            SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                    .getSamplingFrequencyFromCollection(state.getDatabaseName(), c.getTable().getName());
            BigDecimal bigDecimal = EquationsManager.getInstance()
                    .getEquationsFromTimeSeries(state.getDatabaseName(), c.getTable().getName(), c.getName())
                    .genValueByTimestamp(samplingFrequency, state.getOptions().getStartTimestampOfTSData());
            val = c.getType().isInt() ? VMConstant.createIntConstant(bigDecimal.longValue()) :
                    VMConstant.createBigDecimalConstant(bigDecimal);
        } else val = rowVal.getValues().get(c);
        return VMColumnReference.create(c, val);
    }

    @Override
    public VMExpression negatePredicate(VMExpression predicate) {
        return null;
    }

    @Override
    public VMExpression isNull(VMExpression expr) {
        return null;
    }

    @Override
    public List<VMExpression> generateOrderBys() {
        // order by columns
        List<VMColumn> columnsForOrderBy = Randomly.nonEmptySubset(columns);
        List<VMExpression> expressions = columnsForOrderBy.stream().map(column -> {
            VMConstant val;
            if (rowVal == null) {
                val = null;
            } else {
                val = rowVal.getValues().get(column);
            }
            return new VMColumnReference(column, val);
        }).collect(Collectors.toList());
        List<VMExpression> newOrderBys = new ArrayList<>();
        for (VMExpression expr : expressions) {
            if (Randomly.getBoolean()) {
//                VMOrderByTerm newExpr = new VMOrderByTerm(expr, VMOrder.getRandomOrder());
//                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

}
