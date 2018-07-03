package com.dzytsiuk.dbserver.processor.query;

import com.dzytsiuk.dbserver.entity.Query;
import com.dzytsiuk.dbserver.processor.QueryProcessor;
import com.dzytsiuk.dbserver.processor.ResultWriter;
import com.dzytsiuk.dbserver.processor.validator.QuerySemanticsValidator;
import com.dzytsiuk.dbserver.processor.validator.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DefaultQuery {
    private Logger logger = LoggerFactory.getLogger(DefaultQuery.class);


    public void execute(Query query, ResultWriter resultWriter) {
        QuerySemanticsValidator querySemanticsValidator = new QuerySemanticsValidator();
        QueryProcessor queryProcessor = new QueryProcessor();
        StatusCode statusCode = validate(query, querySemanticsValidator);
        if (statusCode == StatusCode.SUCCESS) {
            writeSuccessResult(query, resultWriter, queryProcessor);
        } else {
            resultWriter.writeResult(statusCode);
        }
        logger.info("Query {} executed with status code {}", query.getType().name(), statusCode);
    }

    abstract protected void writeSuccessResult(Query query, ResultWriter resultWriter, QueryProcessor queryProcessor) ;

    abstract protected StatusCode validate(Query query, QuerySemanticsValidator querySemanticsValidator) ;
}
