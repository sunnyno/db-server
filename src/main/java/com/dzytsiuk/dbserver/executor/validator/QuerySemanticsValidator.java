package com.dzytsiuk.dbserver.executor.validator;

import com.dzytsiuk.dbserver.entity.Query;
import com.dzytsiuk.dbserver.executor.validator.sax.SaxQueryHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static com.dzytsiuk.dbserver.server.Server.*;


public class QuerySemanticsValidator {
    private static final SAXParserFactory SAX_PARSER_FACTORY = SAXParserFactory.newInstance();


    public String validateInsert(Query query) {
        return Stream.of(checkDatabase(query), checkTable(query), checkMetadata(query))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    public String validateDelete(Query query) {
        return Stream.of(checkDatabase(query), checkTable(query))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    public String validateUpdate(Query query) {
        return Stream.of(checkDatabase(query), checkTable(query), checkMetadata(query))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    public String validateSelect(Query query) {
        return Stream.of(checkDatabase(query), checkTable(query))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

    }

    public String validateCreateTable(Query query) {
        return Stream.of(checkDatabase(query), checkTableExistence(query))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    public String validateDropTable(Query query) {
        return Stream.of(checkDatabase(query), checkTable(query))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }


    public String validateCreateDatabase(Query query) {
        String checkDatabase = checkDatabaseExistence(query);
        if (checkDatabase != null) return checkDatabase;

        return null;
    }

    public String validateDropDatabase(Query query) {
        String checkDatabase = checkDatabase(query);
        if (checkDatabase != null) return checkDatabase;

        return null;
    }


    private String checkTable(Query query) {
        if (query.getTable() == null) {
            return "No table specified";
        }
        Path table = Paths.get(DB_STORAGE + query.getDataBase() + File.separator + query.getTable() + DATA_XML_SUFFIX);
        if (!Files.exists(table)) {
            return "Table " + query.getTable() + " does not exist";
        }
        return null;
    }

    private String checkTableExistence(Query query) {
        if (query.getTable() == null) {
            return "No table specified";
        }
        Path table = Paths.get(DB_STORAGE + query.getDataBase() + File.separator + query.getTable() + METADATA_XML_SUFFIX);
        if (Files.exists(table)) {
            return "Table " + query.getTable() + " already exist";
        }
        return null;
    }

    private String checkDatabase(Query query) {
        if (query.getDataBase() == null) {
            return "No database specified";
        }
        Path database = Paths.get(DB_STORAGE + query.getDataBase());
        if (!Files.exists(database) || !Files.isDirectory(database)) {
            return "Database " + query.getDataBase() + " does not exist";
        }
        return null;
    }

    private String checkDatabaseExistence(Query query) {
        if (query.getDataBase() == null) {
            return "No database specified";
        }
        Path database = Paths.get(DB_STORAGE + query.getDataBase());
        if (Files.exists(database) || Files.isDirectory(database)) {
            return "Database " + query.getDataBase() + " already exists";
        }
        return null;
    }


    private String checkMetadata(Query query) {
        List<String> actualColumns = query.getMetadata();
        if (actualColumns == null) {
            return "Table " + query.getTable() + " columns are not specified";
        }
        try {
            File metadataFile = new File(DB_STORAGE + query.getDataBase(), query.getTable() + METADATA_XML_SUFFIX);
            List<String> columns = new ArrayList<>();
            SAXParser saxParser = SAX_PARSER_FACTORY.newSAXParser();
            DefaultHandler handler = new SaxQueryHandler(query.getTable(), columns);

            saxParser.parse(metadataFile, handler);

            for (String next : actualColumns) {
                if (!columns.remove(next)) {
                    return "Column " + next + " does not exist";
                }
            }
        } catch (ParserConfigurationException | SAXException | IOException e) {
            return "Metadata validation failed " + e.getMessage();
            //throw new QueryExecuteException("Metadata validation failed", e);
        }
        return null;
    }
}