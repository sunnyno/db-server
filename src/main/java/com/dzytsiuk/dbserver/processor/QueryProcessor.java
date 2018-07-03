package com.dzytsiuk.dbserver.processor;

import com.dzytsiuk.dbserver.entity.Query;
import com.dzytsiuk.dbserver.exception.QueryExecuteException;
import com.dzytsiuk.dbserver.processor.stax.StaxQueryHandler;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static com.dzytsiuk.dbserver.server.Server.*;

public class QueryProcessor {

    private static final StaxQueryHandler STAX_QUERY_HANDLER = new StaxQueryHandler();


    public File select(Query query) {
        String dataBase = query.getDataBase();
        String table = query.getTable();
        return new File(DB_STORAGE + dataBase, table + DATA_XML_SUFFIX);
    }

    public int insert(Query query) {
        try {
            File table = new File(DB_STORAGE + query.getDataBase(), query.getTable() + DATA_XML_SUFFIX);
            if (table.length() == 0) {
                return STAX_QUERY_HANDLER.appendFirstDataElement(table, query);
            } else {
                return STAX_QUERY_HANDLER.appendData(table, query);
            }
        } catch (Exception e) {
            throw new QueryExecuteException("Unable to execute query", e);
        }
    }

    public int update(Query query) {
        try {
            File table = new File(DB_STORAGE + query.getDataBase(), query.getTable() + DATA_XML_SUFFIX);
            return STAX_QUERY_HANDLER.updateData(table, query.getData());
        } catch (XMLStreamException | IOException e) {
            throw new QueryExecuteException("Failed to update table " + query.getTable(), e);
        }

    }

    public int delete(Query query) {
        try {
            File table = new File(DB_STORAGE + query.getDataBase(), query.getTable() + DATA_XML_SUFFIX);
            int count = STAX_QUERY_HANDLER.getCount(table, query.getTable());
            try (PrintWriter printWriter = new PrintWriter(table)) {
                printWriter.write("");
            }
            return count;
        } catch (XMLStreamException | IOException e) {
            throw new QueryExecuteException("Failed to get deleted rows count " + query.getTable(), e);
        }
    }

    public boolean createDatabase(Query query) {
        File file = new File(DB_STORAGE, query.getDataBase());
        return file.mkdir();
    }

    public boolean createTable(Query query) {
        try {
            File metadata = new File(DB_STORAGE + query.getDataBase(), query.getTable() + METADATA_XML_SUFFIX);
            File data = new File(DB_STORAGE + query.getDataBase(), query.getTable() + DATA_XML_SUFFIX);
            StaxQueryHandler staxQueryHandler = new StaxQueryHandler();

            boolean appendMetaData = staxQueryHandler.appendMetaData(metadata, query);
            if (appendMetaData) {
                if (data.createNewFile()) {
                    return true;
                } else {
                    metadata.delete();
                }
            }
            return false;
        } catch (IOException | XMLStreamException e) {
            throw new QueryExecuteException("Failed to create table " + query.getTable(), e);
        }
    }

    public boolean dropDatabase(Query query) {
        try {
            File file = new File(DB_STORAGE + query.getDataBase());
            Files.delete(file.toPath());
            return true;
        } catch (IOException e) {
            throw new QueryExecuteException("Failed to delete database " + query.getDataBase(), e);
        }


    }

    public boolean dropTable(Query query) {
        File data = new File(DB_STORAGE + query.getDataBase(), query.getTable() + DATA_XML_SUFFIX);
        File metadata = new File(DB_STORAGE + query.getDataBase(), query.getTable() + METADATA_XML_SUFFIX);
        try {
            Path tmp = Files.createTempFile(null, null);
            Files.copy(Paths.get(metadata.getAbsolutePath()), tmp, StandardCopyOption.REPLACE_EXISTING);
            if (metadata.delete()) {
                if (data.delete()) {
                    return true;
                } else {
                    File rollbackMetadata = new File(DB_STORAGE + query.getDataBase(),
                            query.getTable() + METADATA_XML_SUFFIX);
                    Files.copy(tmp, Paths.get(rollbackMetadata.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING);
                }
            }
            return false;
        } catch (IOException e) {
            throw new QueryExecuteException("Failed to drop table " + query.getTable(), e);
        }
    }
}
