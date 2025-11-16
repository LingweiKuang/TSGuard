package com.fuzzy.victoriametrics;

import com.alibaba.fastjson.JSONObject;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.benchmark.entity.DBValResultSet;
import com.fuzzy.victoriametrics.apientry.VMApiEntry;
import com.fuzzy.victoriametrics.apientry.VMRequestFactory;
import com.fuzzy.victoriametrics.apientry.VMRequestParam;
import com.fuzzy.victoriametrics.apientry.requesthandler.RequestHandler;
import com.fuzzy.victoriametrics.resultset.VMResultSet;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

@Slf4j
public class VMStatement implements TSFuzzyStatement {
    private VMApiEntry apiEntry;

    public VMStatement(VMApiEntry apiEntry) {
        this.apiEntry = apiEntry;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public void execute(String queryParam) throws SQLException {
        try {
            // 执行数据/时间序列结构更新操作
            VMRequestParam requestParam = JSONObject.parseObject(queryParam, VMRequestParam.class);
            RequestHandler handler = VMRequestFactory.getHandler(requestParam.getType());
            handler.execute(apiEntry, requestParam.getBody());
        } catch (Exception e) {
            log.error("执行操作失败, e:", e);
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public DBValResultSet executeQuery(String queryParam) throws SQLException {
        try {
            // 参数解析
            VMRequestParam requestParam = JSONObject.parseObject(queryParam, VMRequestParam.class);
            RequestHandler handler = VMRequestFactory.getHandler(requestParam.getType());
            String jsonResult = handler.execute(apiEntry, requestParam.getBody());
            return new VMResultSet(jsonResult, requestParam.getType());
        } catch (Exception e) {
            log.error("执行查询失败, queryParam:{} e:", queryParam, e);
            throw new SQLException(e.getMessage());
        }
    }
}
