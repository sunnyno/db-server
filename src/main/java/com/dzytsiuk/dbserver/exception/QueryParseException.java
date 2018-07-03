package com.dzytsiuk.dbserver.exception;

import com.dzytsiuk.dbserver.processor.validator.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryParseException extends RuntimeException {
    private static final Logger logger = LoggerFactory.getLogger(QueryParseException.class);


    private String message;

    public QueryParseException(String message) {
        this.message = message;
        logger.error(message);
    }

    public QueryParseException(String message, Throwable cause) {
        this.message = message;
        logger.error(message, cause);
    }

    public QueryParseException(StatusCode illegalQueryType) {
        this(illegalQueryType.getStatus());
    }

    @Override
    public String getMessage() {
        return message;
    }
}
