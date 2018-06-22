package com.dzytsiuk.dbserver.executor.stax;

import com.dzytsiuk.dbserver.entity.Query;

import javax.xml.stream.*;
import javax.xml.stream.events.XMLEvent;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;

public class StaxQueryHandler {

    private static final XMLOutputFactory XML_OUTPUT_FACTORY = XMLOutputFactory.newInstance();
    private static final XMLEventFactory XML_EVENT_FACTORY = XMLEventFactory.newInstance();

    public int appendFirstDataElement(File table, Query query) throws Exception {


        int count = 0;


        String xmlString = appendDataElement(query, query.getData());

        try (FileWriter fileWriter = new FileWriter(table);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
            bufferedWriter.write(xmlString.toCharArray());
            count++;

        }


        return count;
    }


    public int appendData(File table, Query query) throws IOException, XMLStreamException {
        Path in = Paths.get(table.getAbsolutePath());
        Path temp = Files.createTempFile(null, null);
        int count = 0;
        try (FileWriter out = new FileWriter(temp.toFile())) {

            XMLEventReader reader = XMLInputFactory.newInstance().createXMLEventReader(new FileReader(table));
            XMLEventWriter writer = XMLOutputFactory.newInstance().createXMLEventWriter(out);

            try {
                int depth = 0;
                while (reader.hasNext()) {
                    XMLEvent event = reader.nextEvent();
                    int eventType = event.getEventType();
                    if (eventType == XMLStreamConstants.START_ELEMENT) {
                        depth++;
                    } else if (eventType == XMLStreamConstants.END_ELEMENT) {
                        depth--;
                        if (depth == 0) {

                            //add table
                            writer.add(XML_EVENT_FACTORY.createStartElement("", null, query.getTable()));

                            //add values
                            for (Map.Entry<String, String> colDataMap : query.getData().entrySet()) {
                                String column = colDataMap.getKey();
                                String value = colDataMap.getValue();
                                writer.add(XML_EVENT_FACTORY.createStartElement("", null, column));
                                writer.add(XML_EVENT_FACTORY.createCharacters(value));
                                writer.add(XML_EVENT_FACTORY.createEndElement("", null, column));
                            }

                            writer.add(XML_EVENT_FACTORY.createEndElement("", null, query.getTable()));
                            count++;
                        }
                    }
                    writer.add(event);
                }

            } finally {
                writer.close();
                reader.close();
            }

            Files.move(temp, in, StandardCopyOption.REPLACE_EXISTING);
            return count;
        }

    }


    public boolean appendMetaData(File table, Query query) throws XMLStreamException, IOException {

        int count = 0;

        String xmlString = appendMetaDataElement(query, query.getMetadata());

        try (FileWriter fileWriter = new FileWriter(table);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
            bufferedWriter.write(xmlString.toCharArray());
            count++;

        }

        return count > 0;


    }

    private String appendMetaDataElement(Query query, List<String> metadata) throws XMLStreamException {
        StringWriter stringWriter = new StringWriter();
        XMLStreamWriter xmlStreamWriter =
                XML_OUTPUT_FACTORY.createXMLStreamWriter(stringWriter);
        try {
            xmlStreamWriter.writeStartElement(query.getDataBase());
            xmlStreamWriter.writeStartElement(query.getTable());
            for (String column : metadata) {
                xmlStreamWriter.writeStartElement(column);
                xmlStreamWriter.writeEndElement();
            }
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeEndElement();
        } finally {
            xmlStreamWriter.close();
        }
        return stringWriter.getBuffer().toString();

    }

    private String appendDataElement(Query query, Map<String, String> data) throws XMLStreamException {
        StringWriter stringWriter = new StringWriter();
        XMLStreamWriter xmlStreamWriter =
                XML_OUTPUT_FACTORY.createXMLStreamWriter(stringWriter);
        try {
            xmlStreamWriter.writeStartDocument();
            xmlStreamWriter.writeStartElement(query.getDataBase());
            xmlStreamWriter.writeStartElement(query.getTable());
            for (Map.Entry<String, String> colDataMap : data.entrySet()) {
                String column = colDataMap.getKey();
                String value = colDataMap.getValue();
                xmlStreamWriter.writeStartElement(column);
                xmlStreamWriter.writeCharacters(value);
                xmlStreamWriter.writeEndElement();
            }
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeEndElement();
        } finally {
            xmlStreamWriter.close();
        }
        return stringWriter.getBuffer().toString();
    }


    public int updateData(File table, Map<String, String> data) throws XMLStreamException, IOException {
        Path in = Paths.get(table.getAbsolutePath());
        Path temp = Files.createTempFile(null, null);
        int count = 0;
        try (FileWriter out = new FileWriter(temp.toFile())) {

            XMLEventReader reader = XMLInputFactory.newInstance().createXMLEventReader(new FileReader(table));
            XMLEventWriter writer = XMLOutputFactory.newInstance().createXMLEventWriter(out);

            try {
                XMLEvent prevEvent = null;
                while (reader.hasNext()) {
                    XMLEvent event = reader.nextEvent();
                    for (Map.Entry<String, String> dataMap : data.entrySet()) {
                        if (prevEvent != null && prevEvent.toString().contains(dataMap.getKey())
                                && prevEvent.getEventType() == XMLStreamConstants.START_ELEMENT) {
                            event = XML_EVENT_FACTORY.createCharacters(dataMap.getValue());
                            count++;
                        }
                    }
                    prevEvent = event;
                    writer.add(event);
                }

            } finally {
                writer.close();
                reader.close();
            }

            Files.move(temp, in, StandardCopyOption.REPLACE_EXISTING);
        }
        return count;
    }

    public int getCount(File table, String tableName) throws FileNotFoundException, XMLStreamException {
        int count = 0;
        XMLEventReader reader = XMLInputFactory.newInstance().createXMLEventReader(new FileReader(table));
        try {

            while (reader.hasNext()) {
                XMLEvent event = reader.nextEvent();
                if (event.getEventType() == XMLStreamConstants.START_ELEMENT && event.toString().contains(tableName)) {
                    count++;
                }
            }
        } finally {
            reader.close();
        }
        return count;
    }
}
