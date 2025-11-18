package com.fuzzy.victoriametrics.parser;

import com.fuzzy.common.streamprocessing.constant.TimeSeriesLabelConstant;
import com.fuzzy.victoriametrics.VMSchema;
import com.fuzzy.victoriametrics.ast.*;
import com.fuzzy.victoriametrics.parser.VMLexer.Token;
import com.fuzzy.victoriametrics.parser.VMLexer.TokenType;

import java.math.BigDecimal;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VMParser {
    // VMParser => 语法解析器

    // 拆分时序为 database, table, column
    private static final Pattern TIMESERIES_PARSE_PATTERN = Pattern.compile(
            "^(?<database>[^\\{]+)\\{\\s*table=\"(?<table>[^\"]+)\"\\s*,\\s*timeSeries=\"(?<timeSeries>[^\"]+)\"\\s*\\}$"
    );

    private final List<Token> tokens;
    private int idx = 0;

    public VMParser(List<Token> tokens) {
        this.tokens = tokens;
    }

    private Token peek() {
        return tokens.get(Math.min(idx, tokens.size() - 1));
    }

    private Token consume() {
        return tokens.get(idx++);
    }

    private boolean accept(TokenType t) {
        if (peek().getType() == t) {
            consume();
            return true;
        }
        return false;
    }

    private Token expect(TokenType t) {
        Token tk = peek();
        if (tk.getType() != t) throw new RuntimeException("Expected " + t + " but got " + tk);
        return consume();
    }

    // Expression grammar (descending precedence):
    // expr    := orExpr
    // orExpr  := subExpr ( OR subExpr )*
    // subExpr := addSubExpr  ( '-' addSubExpr )*    // we only implement '-' binary here
    // addSubExpr := unaryExpr  // reserved if more ops added
    // unaryExpr := ( '+' | '-' ) unaryExpr | cmpExpr
    // cmpExpr := primary ( (>=|<=|>|<|==|!=) primary )?
    // primary := NUMBER | series | '(' expr ')'
    // series := IDENT '{' labelList '}'   // labels optional
    public VMExpression parseExpression() {
        return parseLogicalBinaryOp();
    }

    private VMExpression parseLogicalBinaryOp() {
        VMExpression left = parseArithmeticOp();
        // 循环 AND OR UNLESS
        while (peek().getType() == TokenType.OR || peek().getType() == TokenType.AND
                || peek().getType() == TokenType.UNLESS) {
            Token op = consume();
            VMExpression right = parseArithmeticOp();
            left = new VMBinaryLogicalOperation(left, right,
                    VMBinaryLogicalOperation.VMBinaryLogicalOperator.getOperatorByText(op.getText()));
        }
        return left;
    }

    private VMExpression parseArithmeticOp() {
        VMExpression left = parseUnary();
        // 可循环连续 算数 操作
        while (peek().getType() == TokenType.MINUS || peek().getType() == TokenType.PLUS
                || peek().getType() == TokenType.DIVIDE || peek().getType() == TokenType.MULTIPLY) {
            Token op = consume();
            VMExpression right = parseUnary();
            left = new VMBinaryArithmeticOperation(left, right,
                    VMBinaryArithmeticOperation.VMBinaryArithmeticOperator.getOperatorByTextRepresentation(op.getText()));
        }
        return left;
    }

    private VMExpression parseUnary() {
        if (peek().getType() == TokenType.PLUS || peek().getType() == TokenType.MINUS) {
            Token op = consume();
            VMExpression child = parseUnary();
            return new VMUnaryPrefixOperation(child,
                    VMUnaryPrefixOperation.VMUnaryPrefixOperator.getVMUnaryPrefixOperator(op.getText()));
        } else {
            return parseComparisonBinaryOp();
        }
    }

    private VMExpression parseComparisonBinaryOp() {
        VMExpression left = parsePrimary();
        TokenType t = peek().getType();
        if (t == TokenType.GE || t == TokenType.LE || t == TokenType.GT || t == TokenType.LT
                || t == TokenType.EQ || t == TokenType.NE) {
            Token op = consume();
            VMExpression right = parsePrimary();
            return new VMBinaryComparisonOperation(left, right,
                    VMBinaryComparisonOperation.BinaryComparisonOperator.parseBinaryComparisonOperator(op.getText()));
        }
        return left;
    }

    private VMExpression parsePrimary() {
        Token tk = peek();
        if (tk.getType() == TokenType.NUMBER) {
            consume();
            return VMConstant.createBigDecimalConstant(new BigDecimal(tk.getText()));
        }
        if (tk.getType() == TokenType.IDENTIFIER) {
            Token timeSeriesNameTk = consume();
            // example: tsafdb0_value{table="t1", timeSeries="c0"} => database{table="", timeSeries=""}
            String name = timeSeriesNameTk.getText();

            // 正则解析
            String database = "";
            String tableName = "";
            String timeSeriesName = "";
            Matcher matcher = TIMESERIES_PARSE_PATTERN.matcher(name);
            if (matcher.find()) {
                database = matcher.group(TimeSeriesLabelConstant.DATABASE.getLabel());
                tableName = matcher.group(TimeSeriesLabelConstant.TABLE.getLabel());
                timeSeriesName = matcher.group(TimeSeriesLabelConstant.TIME_SERIES.getLabel());
            } else {
                throw new RuntimeException("Invalid primary key expression, can not parse to time series. column name: "
                        + name);
            }

            // 解析为时间序列列
            // TODO GAUGE
            VMSchema.VMColumn VMColumn = new VMSchema.VMColumn(
                    timeSeriesName, false, VMSchema.VMDataType.GAUGE);
            VMSchema.VMTable table = new VMSchema.VMTable(tableName, database,
                    List.of(VMColumn), null);
            VMColumn.setTable(table);
            return new VMColumnReference(VMColumn,
                    VMConstant.createBigDecimalConstant(BigDecimal.ONE));
        }
        if (tk.getType() == TokenType.BOOL) {
            // 忽略标量前缀 bool
            consume();
            return parsePrimary();
        }
        if (tk.getType() == TokenType.LEFT_PAREN) {
            // 左括号
            consume();
            VMExpression inside = parseExpression();
            expect(TokenType.RIGHT_PAREN);
            return inside;
        }
        throw new RuntimeException("Unexpected token in primary: " + tk);
    }
}
