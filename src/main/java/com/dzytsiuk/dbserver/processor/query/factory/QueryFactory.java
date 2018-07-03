package com.dzytsiuk.dbserver.processor.query.factory;

import com.dzytsiuk.dbserver.entity.QueryType;
import com.dzytsiuk.dbserver.exception.QueryParseException;
import com.dzytsiuk.dbserver.processor.query.*;
import com.dzytsiuk.dbserver.processor.validator.StatusCode;

import static com.dzytsiuk.dbserver.entity.QueryType.*;

public class QueryFactory {

    private static final QueryFactory INSTANCE = new QueryFactory();

    private QueryFactory() {
    }

    public static QueryFactory getInstance() {
        return INSTANCE;
    }

    public DefaultQuery getDefaultQuery(QueryType queryType) {
        if (queryType == SELECT) {
            return new SelectQuery();

        } else if (queryType == INSERT) {
            return new InsertQuery();

        } else if (queryType == UPDATE) {
            return new UpdateQuery();

        } else if (queryType == DELETE) {
            return new DeleteQuery();

        } else if (queryType == DROP_DATABASE) {
            return new DropDatabaseQuery();

        } else if (queryType == DROP_TABLE) {
            return new DropTableQuery();

        } else if (queryType == CREATE_DATABASE) {
            return new CreateDatabaseQuery();

        } else if (queryType == CREATE_TABLE) {
            return new CreateTableQuery();

        } else {
            throw new QueryParseException(StatusCode.ILLEGAL_QUERY_TYPE);
        }
    }
}
