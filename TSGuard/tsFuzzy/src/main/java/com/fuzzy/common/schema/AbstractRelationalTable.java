package com.fuzzy.common.schema;

import com.benchmark.entity.DBValResultSet;
import com.fuzzy.IgnoreMeException;
import com.fuzzy.SQLGlobalState;
import com.fuzzy.common.query.SQLQueryAdapter;

import java.util.List;

public class AbstractRelationalTable<C extends AbstractTableColumn<?, ?>, I extends TableIndex, G extends SQLGlobalState<?, ?>>
        extends AbstractTable<C, I, G> {

    public AbstractRelationalTable(String name, String databaseName, List<C> columns, List<I> indexes,
                                   boolean isView) {
        super(name, databaseName, columns, indexes, isView);
    }

    public String getSelectCountTableName() {
        return name;
    }

    public int getSelectCountIndex() {
        return 1;
    }

    public String selectCountStatement() {
        return "SELECT COUNT(*) FROM " + getSelectCountTableName();
    }

    @Override
    public long getNrRows(G globalState) {
        if (rowCount == NO_ROW_COUNT_AVAILABLE) {
            SQLQueryAdapter q = new SQLQueryAdapter(selectCountStatement());
            try (DBValResultSet query = q.executeAndGet(globalState)) {
                if (query == null) {
                    throw new IgnoreMeException();
                }
                // 判断第一条满足数量规则即可
                query.hasNext();
                rowCount = query.getLong(getSelectCountIndex());
                return rowCount;
            } catch (Throwable t) {
                // an exception might be expected, for example, when invalid view is created
                throw new IgnoreMeException();
            }
        } else {
            return rowCount;
        }
    }

}
