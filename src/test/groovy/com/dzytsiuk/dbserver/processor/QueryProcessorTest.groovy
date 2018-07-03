package com.dzytsiuk.dbserver.processor

import com.dzytsiuk.dbserver.entity.Query
import com.dzytsiuk.dbserver.entity.QueryType
import com.dzytsiuk.dbserver.server.Server
import org.junit.Test

import static org.junit.Assert.*

class QueryProcessorTest {

    @Test
    void insertTest() {
        Query query = new Query(type: QueryType.INSERT, dataBase: 'test', table: 'user1', data: ['id': '1', 'name': 'test'])
        File dir = new File(Server.DB_STORAGE, query.dataBase)
        dir.mkdir()
        File file = new File(Server.DB_STORAGE + query.dataBase, query.table + Server.DATA_XML_SUFFIX)
        def beforeSize = file.size()
        QueryProcessor queryExecutor = new QueryProcessor()
        queryExecutor.insert(query)
        def afterSize = file.size()
        assertNotNull(file)
        assertTrue(afterSize > beforeSize)
        file.delete()
        dir.delete()
    }

    @Test
    void updateTest() {
        Query insertQuery = new Query(type: QueryType.INSERT, dataBase: 'test', table: 'user1', data: ['id': '1', 'name': 'test'])
        Query updateQuery = new Query(type: QueryType.UPDATE, dataBase: 'test', table: 'user1', data: ['id': '2'])
        File dir = new File(Server.DB_STORAGE, insertQuery.dataBase)
        dir.mkdir()
        File file = new File(Server.DB_STORAGE + insertQuery.dataBase, insertQuery.table + Server.DATA_XML_SUFFIX)
        file.createNewFile()
        QueryProcessor queryExecutor = new QueryProcessor()
        queryExecutor.insert(insertQuery)
        int affectedRows = queryExecutor.update(updateQuery)
        File fileAfter = new File(Server.DB_STORAGE + insertQuery.dataBase, insertQuery.table + Server.DATA_XML_SUFFIX)
        assertNotEquals(file.getBytes(), fileAfter.getBytes())
        assertEquals(1, affectedRows)
        file.delete()
        fileAfter.delete()
        dir.delete()
    }

    @Test
    void deleteTest() {
        Query insertQuery = new Query(type: QueryType.INSERT, dataBase: 'test', table: 'user1', data: ['id': '1', 'name': 'test'])
        Query deleteQuery = new Query(type: QueryType.DELETE, dataBase: 'test', table: 'user1')
        File dir = new File(Server.DB_STORAGE, insertQuery.dataBase)
        dir.mkdir()
        File file = new File(Server.DB_STORAGE + insertQuery.dataBase + File.separator
                + insertQuery.table + Server.DATA_XML_SUFFIX)
        QueryProcessor queryExecutor = new QueryProcessor()
        queryExecutor.insert(insertQuery)
        int affectedRows = queryExecutor.delete(deleteQuery)

        assertEquals(0, file.getBytes().size())
        assertEquals(1, affectedRows)

        file.delete()
        dir.delete()
    }

    @Test
    void selectTest() {
        Query query = new Query(type: QueryType.SELECT, dataBase: 'test', table: 'user1')
        File dir = new File(Server.DB_STORAGE, query.dataBase)
        dir.mkdir()
        File file = new File(Server.DB_STORAGE + query.dataBase, query.table + Server.DATA_XML_SUFFIX)
        QueryProcessor queryExecutor = new QueryProcessor()
        def select = queryExecutor.select(query)
        assertEquals(file.name, select.name)
        file.delete()
        dir.delete()
    }

    @Test
    void createTable() {
        Query query = new Query(type: QueryType.CREATE_TABLE, dataBase: 'test', table: 'user1', metadata: ['id', 'name'])
        File dir = new File(Server.DB_STORAGE, query.dataBase)
        dir.mkdir()

        File data = new File(Server.DB_STORAGE + query.dataBase, query.table + Server.DATA_XML_SUFFIX)
        File metadata = new File(Server.DB_STORAGE + query.dataBase, query.table + Server.METADATA_XML_SUFFIX)
        QueryProcessor queryExecutor = new QueryProcessor()
        queryExecutor.createTable(query)

        assertNotNull(data)
        assertNotNull(metadata)
        data.delete()
        metadata.delete()
        dir.delete()
    }

    @Test
    void createDatabase() {
        Query query = new Query(type: QueryType.CREATE_DATABASE, dataBase: 'test')

        File database = new File(Server.DB_STORAGE, query.dataBase)
        QueryProcessor queryExecutor = new QueryProcessor()
        queryExecutor.createDatabase(query)

        assertNotNull(database)
        database.delete()

    }

    @Test
    void dropTable() {
        Query query = new Query(type: QueryType.DROP_TABLE, dataBase: 'test', table: 'user1')
        File dir = new File(Server.DB_STORAGE + query.dataBase)
        dir.mkdir()

        File data = new File(Server.DB_STORAGE + query.dataBase, query.table + Server.DATA_XML_SUFFIX)
        data.createNewFile()
        File metadata = new File(Server.DB_STORAGE + query.dataBase, query.table + Server.METADATA_XML_SUFFIX)
        metadata.createNewFile()
        QueryProcessor queryExecutor = new QueryProcessor()
        queryExecutor.dropTable(query)

        assertFalse(data.exists())
        assertFalse(metadata.exists())
        dir.delete()
    }


    @Test
    void dropDatabase() {
        Query query = new Query(type: QueryType.DROP_DATABASE, dataBase: 'test')
        File database = new File(Server.DB_STORAGE + query.dataBase)
        database.mkdir()
        QueryProcessor queryExecutor = new QueryProcessor()
        queryExecutor.dropDatabase(query)
        assertTrue(!database.exists())
    }

}
