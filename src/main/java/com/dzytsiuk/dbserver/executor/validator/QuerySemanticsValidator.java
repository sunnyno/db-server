package com.dzytsiuk.dbserver.executor.validator;

import com.dzytsiuk.dbserver.entity.Query;
import com.dzytsiuk.dbserver.exception.QueryExecuteException;
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

public class QuerySemanticsValidator {
    private static final String DB_STORAGE = "src/main/resources/database/";
    private static final String METADATA_XML_SUFFIX = "-metadata.xml";
    private static final String DATA_XML_SUFFIX = "-data.xml";
    private static final SAXParserFactory SAX_PARSER_FACTORY = SAXParserFactory.newInstance();


    public String validateInsert(Query query) {
        return checkDML(query);
    }

    public String validateDelete(Query query) {
        return checkDBAndTable(query);
    }

    public String validateUpdate(Query query) {
        return checkDML(query);
    }

    public String validateSelect(Query query) {

        return checkDBAndTable(query);

    }

    public String validateCreateTable(Query query) {
        String checkDatabase = checkDatabase(query);
        if (checkDatabase != null) return checkDatabase;


        String checkTable = checkTableExistance(query);
        if (checkTable != null) return checkTable;

        return null;
    }

    public String validateDropTable(Query query) {
        return checkDBAndTable(query);
    }


    public String validateCreateDatabase(Query query) {
        String checkDatabase = checkDatabaseExistance(query);
        if (checkDatabase != null) return checkDatabase;

        return null;
    }

    public String validateDropDatabase(Query query) {
        String checkDatabase = checkDatabase(query);
        if (checkDatabase != null) return checkDatabase;

        return null;
    }


    private String checkDBAndTable(Query query) {
        String checkDatabase = checkDatabase(query);
        if (checkDatabase != null) return checkDatabase;


        String checkTable = checkTable(query);
        if (checkTable != null) return checkTable;

        return null;
    }

    private String checkDML(Query query) {
        checkDBAndTable(query);

        String checkMetadata = checkMetadata(query);
        if (checkMetadata != null) return checkMetadata;
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

    private String checkTableExistance(Query query) {
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

    private String checkDatabaseExistance(Query query) {
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
            File matadataFile = new File(DB_STORAGE + query.getDataBase(), query.getTable() + METADATA_XML_SUFFIX);
            List<String> columns = new ArrayList<>();
            SAXParser saxParser = SAX_PARSER_FACTORY.newSAXParser();
            DefaultHandler handler = new SaxQueryHandler(query.getTable(), columns);

            saxParser.parse(matadataFile, handler);

            for (String next : actualColumns) {
                if (!columns.remove(next)) {
                    return "Column " + next + " does not exist";
                }
            }
        } catch (ParserConfigurationException | SAXException | IOException e) {
            throw new QueryExecuteException("Metadata validation failed", e);
        }
        return null;
    }
}
