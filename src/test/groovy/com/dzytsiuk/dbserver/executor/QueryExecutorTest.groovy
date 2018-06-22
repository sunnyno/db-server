package com.dzytsiuk.dbserver.executor

import com.dzytsiuk.dbserver.entity.Query
import com.dzytsiuk.dbserver.entity.QueryType
import org.junit.Test

import static org.junit.Assert.*

class QueryExecutorTest {

    @Test
    void insertTest() {
        Query query = new Query(type: QueryType.INSERT, dataBase: 'test', table: 'user1', data: ['id': '1', 'name': 'test'])
        File dir = new File("src/main/resources/database/" + query.dataBase)
        dir.mkdir()
        File file = new File("src/main/resources/database/" + query.dataBase, query.table + "-data.xml")
        int beforeSize = file.size()
        QueryExecutor queryExecutor = new QueryExecutor(query)
        queryExecutor.insert()
        int afterSize = file.size()
        assertNotNull(file)
        assertTrue(afterSize > beforeSize)
        file.delete()
        dir.delete()

    }

    @Test
    void updateTest() {
        Query insertQuery = new Query(type: QueryType.INSERT, dataBase: 'test', table: 'user1', data: ['id': '1', 'name': 'test'])
        Query updateQuery = new Query(type: QueryType.UPDATE, dataBase: 'test', table: 'user1', data: ['id': '2'])
        File dir = new File("src/main/resources/database/" + insertQuery.dataBase)
        dir.mkdir()
        File file = new File("src/main/resources/database/" + insertQuery.dataBase, insertQuery.table + "-data.xml")
        file.createNewFile()
        QueryExecutor queryExecutor = new QueryExecutor(insertQuery)
        queryExecutor.insert()

        QueryExecutor update = new QueryExecutor(updateQuery)
        int affectedRows = update.update()
        File fileAfter = new File("src/main/resources/database/" + insertQuery.dataBase, insertQuery.table + "-data.xml")
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
        File dir = new File("src/main/resources/database/" + insertQuery.dataBase)
        dir.mkdir()
        File file = new File("src/main/resources/database/" + insertQuery.dataBase + '/' + insertQuery.table + "-data.xml")
        QueryExecutor queryExecutor = new QueryExecutor(insertQuery)
        queryExecutor.insert()

        QueryExecutor delete = new QueryExecutor(deleteQuery)
        int affectedRows = delete.delete()

        assertEquals(0, file.getBytes().size())
        assertEquals(1, affectedRows)

        file.delete()
        dir.delete()
    }

    @Test
    void selectTest() {
        Query query = new Query(type: QueryType.SELECT, dataBase: 'test', table: 'user1')
        File dir = new File("src/main/resources/database/" + query.dataBase)
        dir.mkdir()
        File file = new File("src/test/resources/db/" + query.dataBase, query.table + "-data.xml")
        QueryExecutor queryExecutor = new QueryExecutor(query)
        def select = queryExecutor.select()
        assertEquals(file.name, select.name)
        file.delete()
        dir.delete()
    }

    @Test
    void createTable() {
        Query query = new Query(type: QueryType.CREATE_TABLE, dataBase: 'test', table: 'user1', metadata: ['id', 'name'])
        File dir = new File("src/main/resources/database/" + query.dataBase)
        dir.mkdir()

        File data = new File("src/main/resources/database/" + query.dataBase, query.table + "-data.xml")
        File metadata = new File("src/main/resources/database/" + query.dataBase, query.table + "-metadata.xml")
        QueryExecutor queryExecutor = new QueryExecutor(query)
        queryExecutor.createTable()

        assertNotNull(data)
        assertNotNull(metadata)
        data.delete()
        metadata.delete()
        dir.delete()
    }

    @Test
    void createDatabase() {
        Query query = new Query(type: QueryType.CREATE_DATABASE, dataBase: 'test')

        File database = new File("src/main/resources/database/" + query.dataBase)
        QueryExecutor queryExecutor = new QueryExecutor(query)
        queryExecutor.createDatabase()

        assertNotNull(database)
        database.delete()

    }

    @Test
    void dropTable() {
        Query query = new Query(type: QueryType.DROP_TABLE, dataBase: 'test', table: 'user1')
        File dir = new File("src/main/resources/database/" + query.dataBase)
        dir.mkdir()

        File data = new File("src/main/resources/database/" + query.dataBase, query.table + "-data.xml")
        data.createNewFile()
        File metadata = new File("src/main/resources/database/" + query.dataBase, query.table + "-metadata.xml")
        metadata.createNewFile()
        QueryExecutor queryExecutor = new QueryExecutor(query)
        queryExecutor.dropTable()

        assertFalse(data.exists())
        assertFalse(metadata.exists())
        dir.delete()
    }


    @Test
    void dropDatabase() {
        Query query = new Query(type: QueryType.DROP_DATABASE, dataBase: 'test')

        File database = new File("src/main/resources/database/" + query.dataBase)
        database.mkdir()
        QueryExecutor queryExecutor = new QueryExecutor(query)
        queryExecutor.dropDatabase()

        assertTrue(!database.exists())

    }

}
