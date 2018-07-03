package com.dzytsiuk.dbserver.processor.validator.sax;


import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.util.List;


public class SaxQueryHandler extends DefaultHandler {

    private String tableName;
    private List<String> columns;
    private boolean isTableTag;

    public SaxQueryHandler(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }


    public void startElement(String uri, String localName, String qName,
                             Attributes attributes) {


        if (qName.equalsIgnoreCase(tableName)) {
            isTableTag = true;
        }
        if (isTableTag) {
            columns.add(qName);
        }

    }

    public void endElement(String uri, String localName, String qName) {
        if (qName.equalsIgnoreCase(tableName)) {
            isTableTag = false;
        }

    }
}
