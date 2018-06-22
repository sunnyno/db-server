package com.dzytsiuk.dbserver.exception;

public class QueryParseException extends RuntimeException {

    public QueryParseException(String message) {
        super(message);
    }

    public QueryParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
