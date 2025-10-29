package com.fuzzy.prometheus.streamcomputing.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class PrometheusLexer {
    // PrometheusLexer => 词法分析器
    private final String s;
    private int p = 0;

    public PrometheusLexer(String s) {
        this.s = s;
    }

    private void skipSpaces() {
        while (p < s.length() && Character.isWhitespace(s.charAt(p))) p++;
    }

    public enum TokenType {
        NUMBER, IDENTIFIER,
        LEFT_BRACE, RIGHT_BRACE, LEFT_PAREN, RIGHT_PAREN, COMMA, STRING,
        EQ, GT, LT, GE, LE, NE,
        PLUS, MINUS, DIVIDE, MULTIPLY,
        OR, AND, UNLESS,
        BOOL,
        EOF
    }

    public static class Token {
        private final TokenType type;
        private final String text;

        Token(TokenType t, String txt) {
            this.type = t;
            this.text = txt;
        }

        public String toString() {
            return type + (text == null ? "" : "('" + text + "')");
        }

        public TokenType getType() {
            return type;
        }

        public String getText() {
            return text;
        }
    }

    public Token next() {
        skipSpaces();
        if (p >= s.length()) return new Token(TokenType.EOF, "");
        char c = s.charAt(p);

        // single char tokens
        switch (c) {
            case '{':
                p++;
                return new Token(TokenType.LEFT_BRACE, "{");
            case '}':
                p++;
                return new Token(TokenType.RIGHT_BRACE, "}");
            case '(':
                p++;
                return new Token(TokenType.LEFT_PAREN, "(");
            case ')':
                p++;
                return new Token(TokenType.RIGHT_PAREN, ")");
            case ',':
                p++;
                return new Token(TokenType.COMMA, ",");
            case '+':
                p++;
                return new Token(TokenType.PLUS, "+");
            case '-':
                p++;
                return new Token(TokenType.MINUS, "-");
            case '/':
                p++;
                return new Token(TokenType.DIVIDE, "/");
            case '*':
                p++;
                return new Token(TokenType.MULTIPLY, "*");
            case '>':
                p++;
                if (p < s.length() && s.charAt(p) == '=') {
                    p++;
                    return new Token(TokenType.GE, ">=");
                }
                return new Token(TokenType.GT, ">");
            case '<':
                p++;
                if (p < s.length() && s.charAt(p) == '=') {
                    p++;
                    return new Token(TokenType.LE, "<=");
                }
                return new Token(TokenType.LT, "<");
            case '!':
                if (p + 1 < s.length() && s.charAt(p + 1) == '=') {
                    p += 2;
                    return new Token(TokenType.NE, "!=");
                }
                break;
            case '=':
                if (p + 1 < s.length() && s.charAt(p + 1) == '=') {
                    p += 2;
                    return new Token(TokenType.EQ, "==");
                }
                break;
            case '"':
                // string literal
                int start = p + 1;
                p++;
                StringBuilder sb = new StringBuilder();
                while (p < s.length()) {
                    char ch = s.charAt(p);
                    if (ch == '"') {
                        p++;
                        break;
                    }
                    if (ch == '\\' && p + 1 < s.length()) {
                        char next = s.charAt(p + 1);
                        sb.append(next);
                        p += 2;
                        continue;
                    }
                    sb.append(ch);
                    p++;
                }
                return new Token(TokenType.STRING, sb.toString());
        }

        // identifiers or keywords OR (series names can start with letters and digits and underscores; but in input series names start with tsafdb_... which matches ident plus digits/underscores)
        if (Character.isLetter(c) || c == '_') {
            // 进入时间序列或 OR, AND, UNLESS 表达式标识范围
            // 先判定 OR, AND, UNLESS
            int st = p;
            if ("OR".equalsIgnoreCase(s.substring(st, st + 2).toUpperCase(Locale.ROOT))) {
                p += 2;
                return new Token(TokenType.OR, s.substring(st, st + 2).toUpperCase(Locale.ROOT));
            }
            if ("AND".equalsIgnoreCase(s.substring(st, st + 3).toUpperCase(Locale.ROOT))) {
                p += 3;
                return new Token(TokenType.AND, s.substring(st, st + 3).toUpperCase(Locale.ROOT));
            }
            if ("UNLESS".equalsIgnoreCase(s.substring(st, st + 6).toUpperCase(Locale.ROOT))) {
                p += 6;
                return new Token(TokenType.UNLESS, s.substring(st, st + 6).toUpperCase(Locale.ROOT));
            }
            if ("bool".equalsIgnoreCase(s.substring(st, st + 4).toUpperCase(Locale.ROOT))) {
                p += 4;
                return new Token(TokenType.BOOL, s.substring(st, st + 4).toUpperCase(Locale.ROOT));
            }

            // 判定时间序列标识符
            while (p < s.length()) {
                char ch = s.charAt(p);
                // 将整行时间序列, 包括标签集合纳入 IDENT 范围
                if (ch == ')') break;
                p++;
            }
            String txt = s.substring(st, p);
            return new Token(TokenType.IDENTIFIER, txt);
        }

        // number (integer or float)
        if (Character.isDigit(c)) {
            int st = p;
            while (p < s.length() && (Character.isDigit(s.charAt(p)) || s.charAt(p) == '.')) p++;
            String num = s.substring(st, p);
            return new Token(TokenType.NUMBER, num);
        }

        // if nothing matched, try to capture long identifiers that include digits and underscores but may include other chars (like tsafdb_95_f3d74f6d_d5e8_4aa7_941e_794f16b7b42d)
        // (we handled those above as IDENT since underscores allowed)
        throw new RuntimeException("Unexpected char at pos " + p + ": '" + c + "'");
    }

    /**
     * 对 promQL 进行词法解析, 形成 List<Token>
     *
     * @param promQL
     * @return
     */
    public static List<Token> tokenize(String promQL) {
        PrometheusLexer lex = new PrometheusLexer(promQL);
        List<PrometheusLexer.Token> tokens = new ArrayList<>();
        PrometheusLexer.Token t;
        do {
            t = lex.next();
            tokens.add(t);
        } while (t.type != PrometheusLexer.TokenType.EOF);
        return tokens;
    }
}
