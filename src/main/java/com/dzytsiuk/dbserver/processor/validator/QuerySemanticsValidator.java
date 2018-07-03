package com.dzytsiuk.dbserver.processor.validator;

import com.dzytsiuk.dbserver.entity.Query;
import com.dzytsiuk.dbserver.exception.QueryExecuteException;
import com.dzytsiuk.dbserver.processor.validator.sax.SaxQueryHandler;
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
import java.util.function.Function;

import static com.dzytsiuk.dbserver.server.Server.*;


public class QuerySemanticsValidator {
    private static final SAXParserFactory SAX_PARSER_FACTORY = SAXParserFactory.newInstance();


    @SafeVarargs
    private final StatusCode validate(Query query, Function<Query, StatusCode>... validateFunction) {
        for (Function<Query, StatusCode> function : validateFunction) {
            StatusCode result = function.apply(query);
            if (result != StatusCode.SUCCESS) {
                return result;
            }
        }
        return StatusCode.SUCCESS;
    }

    public StatusCode validateInsert(Query query) {
        return validate(query, this::checkDatabase, this::checkTable, this::checkMetadata);
    }

    public StatusCode validateDelete(Query query) {
        return validate(query, this::checkDatabase, this::checkTable);
    }

    public StatusCode validateUpdate(Query query) {
        return validate(query, this::checkDatabase, this::checkTable, this::checkMetadata);
    }

    public StatusCode validateSelect(Query query) {
        return validate(query, this::checkDatabase, this::checkTable);
    }

    public StatusCode validateCreateTable(Query query) {
        return validate(query, this::checkDatabase, this::checkTableExistence);
    }

    public StatusCode validateDropTable(Query query) {
        return validate(query, this::checkDatabase, this::checkTable);
    }


    public StatusCode validateCreateDatabase(Query query) {
        return validate(query, this::checkDatabaseExistence);
    }

    public StatusCode validateDropDatabase(Query query) {
        return validate(query, this::checkDatabase);
    }


    private StatusCode checkTable(Query query) {
        if (query.getTable() == null) {
            return StatusCode.NO_TABLE_SPECIFIED;
        }
        Path table = Paths.get(DB_STORAGE + query.getDataBase() + File.separator + query.getTable() + DATA_XML_SUFFIX);
        if (!Files.exists(table)) {
            return StatusCode.TABLE_DOES_NOT_EXIST;
        }
        return StatusCode.SUCCESS;
    }

    private StatusCode checkTableExistence(Query query) {
        if (query.getTable() == null) {
            return StatusCode.NO_TABLE_SPECIFIED;
        }
        Path table = Paths.get(DB_STORAGE + query.getDataBase() + File.separator + query.getTable() + METADATA_XML_SUFFIX);
        if (Files.exists(table)) {
            return StatusCode.TABLE_ALREADY_EXISTS;
        }
        return StatusCode.SUCCESS;
    }

    private StatusCode checkDatabase(Query query) {
        if (query.getDataBase() == null) {
            return StatusCode.NO_DATABASE_SPECIFIED;
        }
        Path database = Paths.get(DB_STORAGE + query.getDataBase());
        if (!Files.exists(database) || !Files.isDirectory(database)) {
            return StatusCode.DATABASE_DOES_NOT_EXIST;
        }
        return StatusCode.SUCCESS;
    }

    private StatusCode checkDatabaseExistence(Query query) {
        if (query.getDataBase() == null) {
            return StatusCode.NO_DATABASE_SPECIFIED;
        }
        Path database = Paths.get(DB_STORAGE + query.getDataBase());
        if (Files.exists(database) || Files.isDirectory(database)) {
            return StatusCode.DATABASE_ALREADY_EXISTS;
        }
        return StatusCode.SUCCESS;
    }


    private StatusCode checkMetadata(Query query) {
        List<String> actualColumns = query.getMetadata();
        if (actualColumns == null) {
            return StatusCode.NO_COLUMNS_SPECIFIED;
        }
        try {
            File metadataFile = new File(DB_STORAGE + query.getDataBase(), query.getTable() + METADATA_XML_SUFFIX);
            List<String> columns = new ArrayList<>();
            SAXParser saxParser = SAX_PARSER_FACTORY.newSAXParser();
            DefaultHandler handler = new SaxQueryHandler(query.getTable(), columns);

            saxParser.parse(metadataFile, handler);

            for (String next : actualColumns) {
                if (!columns.remove(next)) {
                    return StatusCode.COLUMN_DOES_NOT_EXIST;
                }
            }
        } catch (ParserConfigurationException | SAXException | IOException e) {
            throw new QueryExecuteException(StatusCode.INVALID_METADATA, e);
        }
        return StatusCode.SUCCESS;
    }
}