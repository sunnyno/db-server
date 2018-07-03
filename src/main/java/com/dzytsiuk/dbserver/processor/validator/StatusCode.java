package com.dzytsiuk.dbserver.processor.validator;

public enum StatusCode {
    NO_TABLE_SPECIFIED("No table specified"), NO_DATABASE_SPECIFIED("No database specified"),
    TABLE_DOES_NOT_EXIST("Table does not exist"), DATABASE_DOES_NOT_EXIST("Database does not exist"),
    TABLE_ALREADY_EXISTS("Table already exist"), DATABASE_ALREADY_EXISTS("Database already exists"),
    NO_COLUMNS_SPECIFIED("Table columns are not specified"), COLUMN_DOES_NOT_EXIST("Column does not exist"),
    INVALID_METADATA("Metadata validation failed "), ILLEGAL_QUERY_TYPE("Illegal query type"), SUCCESS("Success");

    private String status;

    StatusCode(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

}
