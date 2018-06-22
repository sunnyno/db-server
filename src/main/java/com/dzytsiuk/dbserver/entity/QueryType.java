package com.dzytsiuk.dbserver.entity;

public enum QueryType {
    SELECT("select"), INSERT("insert"), UPDATE("update"), DELETE("delete"), CREATE_TABLE("create_table"), CREATE_DATABASE("create_database"),
    DROP_TABLE("drop_table"), DROP_DATABASE("drop_database"), ERROR("error");

    private String type;

    QueryType(String type) {
        this.type = type;
    }

    public static QueryType getQueryTypeByName(String name) {
        for (QueryType queryType : QueryType.values()) {
            if (queryType.type.equalsIgnoreCase(name)) {
                return queryType;
            }
        }
        return ERROR;
    }

}
