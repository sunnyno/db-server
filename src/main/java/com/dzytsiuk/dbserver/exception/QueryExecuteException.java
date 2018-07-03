package com.dzytsiuk.dbserver.exception;

import com.dzytsiuk.dbserver.processor.validator.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExecuteException extends RuntimeException {
    private static final Logger logger = LoggerFactory.getLogger(QueryParseException.class);
    String message;

    public QueryExecuteException(String message, Throwable cause) {
        this.message = message;
        logger.error(message, cause);
    }

    public QueryExecuteException(StatusCode invalidMetadata, Exception e) {
        this(invalidMetadata.getStatus(), e);
    }

    @Override
    public String getMessage() {
        return message;
    }
}
