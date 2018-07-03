package com.dzytsiuk.dbserver.processor.query;

import com.dzytsiuk.dbserver.entity.Query;
import com.dzytsiuk.dbserver.processor.QueryProcessor;
import com.dzytsiuk.dbserver.processor.ResultWriter;
import com.dzytsiuk.dbserver.processor.validator.QuerySemanticsValidator;
import com.dzytsiuk.dbserver.processor.validator.StatusCode;

public class CreateTableQuery extends DefaultQuery {

    @Override
    protected void writeSuccessResult(Query query, ResultWriter resultWriter, QueryProcessor queryProcessor) {
        resultWriter.writeResult(queryProcessor.createTable(query));
    }

    @Override
    protected StatusCode validate(Query query, QuerySemanticsValidator querySemanticsValidator) {
        return querySemanticsValidator.validateCreateTable(query);
    }
}
