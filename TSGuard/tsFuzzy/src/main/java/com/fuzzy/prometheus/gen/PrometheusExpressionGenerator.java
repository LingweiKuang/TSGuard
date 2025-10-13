package com.fuzzy.prometheus.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.gen.UntypedExpressionGenerator;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.prometheus.PrometheusGlobalState;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusColumn;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusDataType;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusRowValue;
import com.fuzzy.prometheus.ast.*;
import com.fuzzy.prometheus.ast.PrometheusBinaryComparisonOperation.BinaryComparisonOperator;
import com.fuzzy.prometheus.ast.PrometheusBinaryLogicalOperation.PrometheusBinaryLogicalOperator;
import com.fuzzy.prometheus.ast.PrometheusUnaryPrefixOperation.PrometheusUnaryPrefixOperator;
import com.fuzzy.prometheus.feedback.PrometheusQuerySynthesisFeedbackManager;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class PrometheusExpressionGenerator extends UntypedExpressionGenerator<PrometheusExpression, PrometheusColumn> {

    // 父节点后不允许跟子节点
    public static final Set<String> pairingProhibited = new HashSet<>();
    private final PrometheusGlobalState state;
    private PrometheusRowValue rowVal;

    public PrometheusExpressionGenerator(PrometheusGlobalState state) {
        this.state = state;
        if (state.getOptions().useSyntaxValidator()) initGenerator();
    }

    public PrometheusExpressionGenerator setRowVal(PrometheusRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

    private void initGenerator() {
        // TODO
        // 查询有效性（靠标记规避语法不符合的节点）
        // 经过严格配对实验，以下各种组合均属于语法错误
//        pairingProhibited.add(genHashKeyWithPairActions(Actions.LITERAL, Actions.LITERAL));
    }

    private String genHashKeyWithPairActions(final Actions parentNode, final Actions childNode) {
        return String.format("%s__%s", parentNode.name(), childNode.name());
    }

    private enum Actions {
        COLUMN, LITERAL, UNARY_PREFIX_OPERATION,
        BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON_OPERATION, BINARY_ARITHMETIC_OPERATION,
//        UNARY_NOT_PREFIX_OPERATION,
//        BETWEEN_OPERATOR(true), CAST_OPERATOR(false),
//        BINARY_OPERATION(false), IN_OPERATION(true),
//        UNARY_POSTFIX(true), COMPUTABLE_FUNCTION(false),
        ;
    }

    @Override
    public PrometheusExpression generateExpression(int depth) {
        return null;
    }

    @Override
    protected PrometheusExpression generateExpression(Object parentActions, int depth) {
        try {
            if (depth >= (state.getOptions().useSyntaxSequence() ?
                    PrometheusQuerySynthesisFeedbackManager.expressionDepth.get() :
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

    private PrometheusExpression generateSpecifiedExpression(Actions actions, Object parentActions, int depth) {
        PrometheusExpression expression;
        switch (actions) {
            case COLUMN:
                expression = generateColumn();
                break;
            case LITERAL:
                expression = generateConstant();
                break;
//            case UNARY_NOT_PREFIX_OPERATION:
//                PrometheusExpression subExpression = generateExpression(actions, depth + 1);
//                expression = new PrometheusUnaryNotPrefixOperation(subExpression,
//                        PrometheusUnaryNotPrefixOperation.PrometheusUnaryNotPrefixOperator.getRandom(subExpression));
//                break;
            case UNARY_PREFIX_OPERATION:
                expression = new PrometheusUnaryPrefixOperation(generateExpression(actions, depth + 1),
//                        PrometheusUnaryPrefixOperator.getRandom()
                        PrometheusUnaryPrefixOperator.PLUS
                );
                break;
//            case CAST_OPERATOR:
//                PrometheusExpression columnExpr = generateColumn();
//                expression = new PrometheusCastOperation(columnExpr,
//                        PrometheusCastOperation.CastType.getRandom(columnExpr.getExpectedValue().getType()));
//                break;
//            case UNARY_POSTFIX:
//                expression = new PrometheusUnaryPostfixOperation(generateExpression(actions, depth + 1),
//                        Randomly.fromOptions(PrometheusUnaryPostfixOperation.UnaryPostfixOperator.values()),
//                        Randomly.getBoolean());
//                break;
//            case COMPUTABLE_FUNCTION:
//                expression = getComputableFunction();
//                break;
            case BINARY_LOGICAL_OPERATOR:
                // set operator "and" not allowed in binary scalar expression
                PrometheusExpression leftExpression = generateExpression(actions, depth + 1);
                while (leftExpression.isScalarExpression()) {
                    leftExpression = generateExpression(actions, depth + 1);
                }
                PrometheusExpression rightExpression = generateExpression(actions, depth + 1);
                while (rightExpression.isScalarExpression()) {
                    rightExpression = generateExpression(actions, depth + 1);
                }

                expression = new PrometheusBinaryLogicalOperation(leftExpression, rightExpression,
                        PrometheusBinaryLogicalOperator.getRandom());
                break;
            case BINARY_COMPARISON_OPERATION:
                expression = new PrometheusBinaryComparisonOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        BinaryComparisonOperator.getRandom());
                break;
//            case IN_OPERATION:
//                PrometheusExpression expr = generateExpression(actions, depth + 1);
//                List<PrometheusExpression> rightList = new ArrayList<>();
//                for (int i = 0; i < 1 + Randomly.smallNumber(); i++)
//                    // TODO reported bug: IN 类型不一致处理逻辑存在问题, 改为查找类型一致
//                    rightList.add(generateConstantForPrometheusDataTypeForTSAF(expr.getExpectedValue().getType()));
//                expression = new PrometheusInOperation(expr, rightList, Randomly.getBoolean());
//                break;
//            case BINARY_OPERATION:
//                expression = new PrometheusBinaryOperation(
//                        generateExpression(actions, depth + 1),
//                        generateExpression(actions, depth + 1),
//                        PrometheusBinaryOperator.getRandom());
//                break;
            case BINARY_ARITHMETIC_OPERATION:
                expression = new PrometheusBinaryArithmeticOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        PrometheusBinaryArithmeticOperation.PrometheusBinaryArithmeticOperator.getRandom());
                break;
//            case BETWEEN_OPERATOR:
//                expression = new PrometheusBetweenOperation(generateExpression(actions, depth + 1),
//                        generateExpression(actions, depth + 1),
//                        generateExpression(actions, depth + 1), false);
//                break;
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

    @Override
    public PrometheusExpression generateExpressionForSyntaxValidity(String fatherActions, String childActions) {
        return null;
//        if (ObjectUtils.isEmpty(fatherActions)) return generateLeafNode();
//        Actions father = Actions.valueOf(fatherActions);
//
//        try {
//            switch (father) {
//                case COLUMN:
//                    // Prometheus 不支持 cross join, 故 使用AND操作替代, AND 操作不支持COLUMN、LITERAL单列出现, 故parentActions不应为null
//                    return generateColumn();
//                case LITERAL:
//                    return generateConstant();
//                case UNARY_NOT_PREFIX_OPERATION:
//                    PrometheusExpression subExpression = generateExpressionForSyntaxValidity(childActions, null);
//                    return new PrometheusUnaryNotPrefixOperation(subExpression,
//                            PrometheusUnaryNotPrefixOperation.PrometheusUnaryNotPrefixOperator.getRandom(subExpression));
//                case UNARY_PREFIX_OPERATION:
//                    PrometheusExpression subExpr = generateExpressionForSyntaxValidity(childActions, null);
//                    PrometheusUnaryPrefixOperator random = PrometheusUnaryPrefixOperator.getRandom();
//                    return new PrometheusUnaryPrefixOperation(subExpr, random);
////                case CAST_OPERATOR:
////                    PrometheusExpression columnExpr = generateColumn();
////                    return new PrometheusCastOperation(columnExpr,
////                            PrometheusCastOperation.CastType.getRandom(columnExpr.getExpectedValue().getType()));
////                case UNARY_POSTFIX:
////                    return new PrometheusUnaryPostfixOperation(generateExpressionForSyntaxValidity(childActions, null),
////                            Randomly.fromOptions(PrometheusUnaryPostfixOperation.UnaryPostfixOperator.values()),
////                            Randomly.getBoolean());
////                case COMPUTABLE_FUNCTION:
////                    return generateExpressionForSyntaxValidity(fatherActions, childActions);
//                case BINARY_LOGICAL_OPERATOR:
//                    return new PrometheusBinaryLogicalOperation(
//                            generateExpressionForSyntaxValidity(childActions, null),
//                            generateExpressionForSyntaxValidity(childActions, null),
//                            PrometheusBinaryLogicalOperator.getRandom());
//                case BINARY_COMPARISON_OPERATION:
//                    return new PrometheusBinaryComparisonOperation(
//                            generateExpressionForSyntaxValidity(childActions, null),
//                            generateExpressionForSyntaxValidity(childActions, null),
//                            BinaryComparisonOperator.getRandom());
////                case IN_OPERATION:
////                    PrometheusExpression expr = generateExpressionForSyntaxValidity(childActions, null);
////                    List<PrometheusExpression> rightList = new ArrayList<>();
////                    for (int i = 0; i < 1 + Randomly.smallNumber(); i++)
////                        rightList.add(generateLeafNode());
////                    return new PrometheusInOperation(expr, rightList, Randomly.getBoolean());
////                case BINARY_OPERATION:
////                    return new PrometheusBinaryOperation(
////                            generateExpressionForSyntaxValidity(childActions, null),
////                            generateExpressionForSyntaxValidity(childActions, null),
////                            PrometheusBinaryOperator.getRandom());
//                case BINARY_ARITHMETIC_OPERATION:
//                    return new PrometheusBinaryArithmeticOperation(
//                            generateExpressionForSyntaxValidity(childActions, null),
//                            generateExpressionForSyntaxValidity(childActions, null),
//                            PrometheusBinaryArithmeticOperator.getRandom());
////                case BETWEEN_OPERATOR:
////                    return new PrometheusBetweenOperation(generateExpressionForSyntaxValidity(childActions, null),
////                            generateLeafNode(), generateLeafNode(), false);
//                default:
//                    throw new AssertionError();
//            }
//        } catch (ReGenerateExpressionException e) {
//            return generateExpressionForSyntaxValidity(fatherActions, childActions);
//        }
    }

    private PrometheusExpression ignoreThisExpr(Object parentActions, Actions action, int depth) {
        return generateExpression(parentActions, depth);
    }

    private PrometheusExpression getComputableFunction() {
//        PrometheusComputableFunction.PrometheusFunction func = PrometheusComputableFunction.PrometheusFunction.getRandomFunction();
//        int nrArgs = func.getNrArgs();
//        if (func.isVariadic()) nrArgs += Randomly.smallNumber();
//        PrometheusExpression[] args = new PrometheusExpression[nrArgs];
//        for (int i = 0; i < args.length; i++) {
//            args[i] = func.limitValueRange((PrometheusConstant) generateConstant());
//        }
//        return new PrometheusComputableFunction(func, args);
        return null;
    }

    @Override
    public PrometheusExpression generateConstant() {
        PrometheusDataType[] values;
        if (state.usesPQS()) {
            values = PrometheusDataType.valuesPQS();
        } else if (state.usesTSAF() || state.usesStreamComputing()) {
            values = PrometheusDataType.valuesTSAFOrStreamComputing();
        } else {
            values = PrometheusDataType.values();
        }
        // TODO 仅返回 -1000～1000值, 防止乘法范围溢出
        switch (Randomly.fromOptions(values)) {
            case GAUGE:
                return PrometheusConstant.createIntConstant(state.getRandomly().getInteger(-1000, 1000));
//            case DOUBLE:
//                double val = BigDecimal.valueOf((double) state.getRandomly().getInteger()
//                        / state.getRandomly().getInteger()).setScale(
//                        PrometheusDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
//                return PrometheusDoubleConstant.createDoubleConstant(val);
            default:
                throw new AssertionError();
        }
    }

    public PrometheusExpression generateConstantForPrometheusDataType(PrometheusDataType PrometheusDataType) {
//        switch (PrometheusDataType) {
//            case INT:
//                return PrometheusConstant.createInt32Constant(state.getRandomly().getInteger());
//            case UINT:
//                return PrometheusConstant.createUInt32Constant(state.getRandomly().getUnsignedInteger());
//            case BIGINT:
//                return PrometheusConstant.createInt64Constant(state.getRandomly().getInteger());
//            case UBIGINT:
//                return PrometheusConstant.createUInt64Constant(state.getRandomly().getUnsignedInteger());
//            case BOOL:
//                return PrometheusConstant.createBoolean(Randomly.getBoolean());
//            case BINARY:
//            case VARCHAR:
//                String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
//                return PrometheusConstant.createStringConstant(string);
//            case FLOAT:
//            case DOUBLE:
//            case BIGDECIMAL:
//                double val = BigDecimal.valueOf((double) state.getRandomly().getInteger()
//                        / state.getRandomly().getInteger()).setScale(
//                        PrometheusDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
//                return PrometheusDoubleConstant.createDoubleConstant(val);
//            default:
//                throw new AssertionError();
//        }
        return null;
    }

    public PrometheusExpression generateConstantForPrometheusDataTypeForTSAF(PrometheusDataType PrometheusDataType) {
//        switch (PrometheusDataType) {
//            case BINARY:
//            case VARCHAR:
//            case INT:
//                return PrometheusConstant.createInt32Constant(state.getRandomly().getInteger());
//            case UINT:
//                return PrometheusConstant.createUInt32Constant(state.getRandomly().getUnsignedInteger());
//            case BIGINT:
//                return PrometheusConstant.createInt64Constant(state.getRandomly().getInteger());
//            case UBIGINT:
//                return PrometheusConstant.createUInt64Constant(state.getRandomly().getUnsignedInteger());
//            case BOOL:
//                return PrometheusConstant.createBoolean(Randomly.getBoolean());
//            case FLOAT:
//            case DOUBLE:
//            case BIGDECIMAL:
//                double val = BigDecimal.valueOf((double) state.getRandomly().getInteger()
//                        / state.getRandomly().getInteger()).setScale(
//                        PrometheusDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
//                return PrometheusDoubleConstant.createDoubleConstant(val);
//            case NULL:
//                return PrometheusConstant.createNullConstant();
//            default:
//                throw new AssertionError(String.format("%s", PrometheusDataType));
//        }
        return null;
    }

    @Override
    protected PrometheusExpression generateColumn() {
        PrometheusColumn c = Randomly.fromList(columns);
        PrometheusConstant val = null;
        if (rowVal == null) {
            // TSQS生成表达式时, rowVal默认值为1, 列存在因子时能够进行区分
            SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                    .getSamplingFrequencyFromCollection(state.getDatabaseName(), c.getTable().getName());
            BigDecimal bigDecimal = EquationsManager.getInstance()
                    .getEquationsFromTimeSeries(state.getDatabaseName(), c.getTable().getName(), c.getName())
                    .genValueByTimestamp(samplingFrequency, state.getOptions().getStartTimestampOfTSData());
            val = c.getType().isInt() ? PrometheusConstant.createIntConstant(bigDecimal.longValue()) :
                    PrometheusConstant.createBigDecimalConstant(bigDecimal);
        } else val = rowVal.getValues().get(c);
        return PrometheusColumnReference.create(c, val);
    }

    @Override
    public PrometheusExpression negatePredicate(PrometheusExpression predicate) {
        return null;
//        return new PrometheusUnaryPrefixOperation(predicate, PrometheusUnaryNotPrefixOperator.NOT);
    }

    @Override
    public PrometheusExpression isNull(PrometheusExpression expr) {
        return null;
//        return new PrometheusUnaryPostfixOperation(expr, PrometheusUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    public List<PrometheusExpression> generateOrderBys() {
        // order by columns
        List<PrometheusColumn> columnsForOrderBy = Randomly.nonEmptySubset(columns);
        List<PrometheusExpression> expressions = columnsForOrderBy.stream().map(column -> {
            PrometheusConstant val;
            if (rowVal == null) {
                val = null;
            } else {
                val = rowVal.getValues().get(column);
            }
            return new PrometheusColumnReference(column, val);
        }).collect(Collectors.toList());
        List<PrometheusExpression> newOrderBys = new ArrayList<>();
        for (PrometheusExpression expr : expressions) {
            if (Randomly.getBoolean()) {
//                PrometheusOrderByTerm newExpr = new PrometheusOrderByTerm(expr, PrometheusOrder.getRandomOrder());
//                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

}
