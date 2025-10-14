package com.fuzzy.prometheus.streamcomputing.parser;

import com.fuzzy.prometheus.PrometheusSchema;
import com.fuzzy.prometheus.ast.*;
import com.fuzzy.prometheus.streamcomputing.parser.PrometheusLexer.Token;
import com.fuzzy.prometheus.streamcomputing.parser.PrometheusLexer.TokenType;

import java.math.BigDecimal;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PrometheusParser {
    // PrometheusParser => 语法解析器

    // 拆分时序为 database, table, column
    private static final Pattern TIMESERIES_PARSE_PATTERN = Pattern.compile(
            "^(?<database>[^\\{]+)\\{\\s*table=\"(?<table>[^\"]+)\"\\s*,\\s*timeSeries=\"(?<timeSeries>[^\"]+)\"\\s*\\}$"
    );

    private final List<Token> tokens;
    private int idx = 0;

    public PrometheusParser(List<Token> tokens) {
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
    public PrometheusExpression parseExpression() {
        return parseOr();
    }

    private PrometheusExpression parseOr() {
        PrometheusExpression left = parseArithmetic();
        // 循环 AND OR UNLESS
        while (peek().getType() == TokenType.OR || peek().getType() == TokenType.AND
                || peek().getType() == TokenType.UNLESS) {
            Token op = consume();
            PrometheusExpression right = parseArithmetic();
            left = new PrometheusBinaryLogicalOperation(left, right,
                    PrometheusBinaryLogicalOperation.PrometheusBinaryLogicalOperator.getOperatorByText(op.getText()));
        }
        return left;
    }

    private PrometheusExpression parseArithmetic() {
        PrometheusExpression left = parseUnary();
        // 可循环连续 算数 操作
        while (peek().getType() == TokenType.MINUS || peek().getType() == TokenType.PLUS
                || peek().getType() == TokenType.DIVIDE || peek().getType() == TokenType.MULTIPLY) {
            Token op = consume();
            PrometheusExpression right = parseUnary();
            left = new PrometheusBinaryArithmeticOperation(left, right,
                    PrometheusBinaryArithmeticOperation.PrometheusBinaryArithmeticOperator.getOperatorByTextRepresentation(op.getText()));
        }
        return left;
    }

    private PrometheusExpression parseUnary() {
        if (peek().getType() == TokenType.PLUS || peek().getType() == TokenType.MINUS) {
            Token op = consume();
            PrometheusExpression child = parseUnary();
            return new PrometheusUnaryPrefixOperation(child,
                    PrometheusUnaryPrefixOperation.PrometheusUnaryPrefixOperator.getPrometheusUnaryPrefixOperator(op.getText()));
        } else {
            return parseComparison();
        }
    }

    private PrometheusExpression parseComparison() {
        PrometheusExpression left = parsePrimary();
        TokenType t = peek().getType();
        if (t == TokenType.GE || t == TokenType.LE || t == TokenType.GT || t == TokenType.LT
                || t == TokenType.EQ || t == TokenType.NE) {
            Token op = consume();
            PrometheusExpression right = parsePrimary();
            return new PrometheusBinaryComparisonOperation(left, right,
                    PrometheusBinaryComparisonOperation.BinaryComparisonOperator.parseBinaryComparisonOperator(op.getText()));
        }
        return left;
    }

    private PrometheusExpression parsePrimary() {
        Token tk = peek();
        if (tk.getType() == TokenType.NUMBER) {
            consume();
            return PrometheusConstant.createBigDecimalConstant(new BigDecimal(tk.getText()));
        }
        if (tk.getType() == TokenType.IDENTIFIER) {
            Token timeSeriesNameTk = consume();
            // example: tsafdb_95_f3d74f6d_d5e8_4aa7_941e_794f16b7b42d{table="t1", timeSeries="c0"} => database{table="", timeSeries=""}
            String name = timeSeriesNameTk.getText();

            // 正则解析
            String database = "";
            String tableName = "";
            String timeSeriesName = "";
            Matcher matcher = TIMESERIES_PARSE_PATTERN.matcher(name);
            if (matcher.find()) {
                database = matcher.group("database");
                tableName = matcher.group("table");
                timeSeriesName = matcher.group("timeSeries");
            } else {
                throw new RuntimeException("Invalid primary key expression, can not parse to time series. column name: "
                        + name);
            }

            // 解析为时间序列列
            PrometheusSchema.PrometheusColumn prometheusColumn = new PrometheusSchema.PrometheusColumn(
                    timeSeriesName, false, PrometheusSchema.PrometheusDataType.GAUGE);
            PrometheusSchema.PrometheusTable table = new PrometheusSchema.PrometheusTable(tableName, database,
                    List.of(prometheusColumn), null);
            prometheusColumn.setTable(table);
            return new PrometheusColumnReference(prometheusColumn,
                    PrometheusConstant.createBigDecimalConstant(BigDecimal.ONE));
        }
        if (tk.getType() == TokenType.LEFT_PAREN) {
            // 左括号
            consume();
            PrometheusExpression inside = parseExpression();
            expect(TokenType.RIGHT_PAREN);
            return inside;
        }
        throw new RuntimeException("Unexpected token in primary: " + tk);
    }
}
