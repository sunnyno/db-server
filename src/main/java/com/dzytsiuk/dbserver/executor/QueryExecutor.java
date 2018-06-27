package com.dzytsiuk.dbserver.executor;

import com.dzytsiuk.dbserver.entity.Query;
import com.dzytsiuk.dbserver.exception.QueryExecuteException;
import com.dzytsiuk.dbserver.executor.stax.StaxQueryHandler;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import static com.dzytsiuk.dbserver.server.Server.*;

public class QueryExecutor {

    private static final StaxQueryHandler STAX_QUERY_HANDLER = new StaxQueryHandler();


    public File select(Query query) {
        String dataBase = query.getDataBase();
        String table = query.getTable();
        return new File(DB_STORAGE + dataBase + File.separator + table + DATA_XML_SUFFIX);
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
            File table = new File(DB_STORAGE + query.getDataBase() + File.separator
                    + query.getTable() + DATA_XML_SUFFIX);
            return STAX_QUERY_HANDLER.updateData(table, query.getData());
        } catch (XMLStreamException | IOException e) {
            throw new QueryExecuteException("Failed to update table " + query.getTable(), e);
        }

    }

    public int delete(Query query) {
        try {
            File table = new File(DB_STORAGE + query.getDataBase(), query.getTable() + DATA_XML_SUFFIX);
            int count = STAX_QUERY_HANDLER.getCount(table, query.getTable());
            PrintWriter printWriter = new PrintWriter(table);
            printWriter.write("");
            return count;
        } catch (FileNotFoundException | XMLStreamException e) {
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

            return staxQueryHandler.appendMetaData(metadata, query) && data.createNewFile();
        } catch (IOException | XMLStreamException e) {
            throw new QueryExecuteException("Failed to create table " + query.getTable(), e);
        }
    }

    public boolean dropDatabase(Query query) {
        File file = new File(DB_STORAGE + query.getDataBase());
        return file.delete();

    }

    public boolean dropTable(Query query) {
        File data = new File(DB_STORAGE + query.getDataBase(), query.getTable() + DATA_XML_SUFFIX);
        File metadata = new File(DB_STORAGE + query.getDataBase(), query.getTable() + METADATA_XML_SUFFIX);
        return data.delete() && metadata.delete();
    }
}
