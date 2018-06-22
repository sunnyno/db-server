package com.dzytsiuk.dbserver.exception;

public class QueryExecuteException extends RuntimeException {

    public QueryExecuteException(String message) {
        super(message);
    }

    public QueryExecuteException(String message, Throwable cause) {
        super(message, cause);
    }
}
