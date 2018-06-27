package com.dzytsiuk.dbserver.executor

import com.dzytsiuk.dbserver.entity.Query
import com.dzytsiuk.dbserver.entity.QueryType
import org.junit.Test

import static org.junit.Assert.*;

class QueryParserTest {

    @Test
    void parseInsertTest() {
        Query expectedQuery = new Query(type: QueryType.INSERT, dataBase: 'test', table: 'user1', data: ['id': '1', 'name': 'test'], metadata: ['id', 'name'])
        def jsonQuery = " {\"type\" : \"insert\", \"database\" :\"test\", \"table\":\"user1\", \"data\": {\"id\":\"1\", \"name\":\"test\"}}"

        QueryParser queryParser = new QueryParser(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(jsonQuery.getBytes()))))
        def actualQuery = queryParser.parseQuery()

        assertEquals(expectedQuery.type, actualQuery.type)
        assertEquals(expectedQuery.table, actualQuery.table)
        assertEquals(expectedQuery.dataBase, actualQuery.dataBase)
        expectedQuery.data.each { assertNotNull(actualQuery.data.remove(it.key)) }
        expectedQuery.metadata.each { assertTrue(actualQuery.metadata.remove(it)) }

    }


    @Test
    void parseSelectTest() {
        Query expectedQuery = new Query(type: QueryType.SELECT, dataBase: 'test', table: 'user1')
        def jsonQuery = " {\"type\" : \"select\", \"database\" :\"test\", \"table\":\"user1\"}"

        QueryParser queryParser = new QueryParser(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(jsonQuery.getBytes()))))
        def actualQuery = queryParser.parseQuery()

        assertEquals(expectedQuery, actualQuery)


    }

    @Test
    void parseUpdateTest() {
        Query expectedQuery = new Query(type: QueryType.UPDATE, dataBase: 'test', table: 'user1', data: ['id': '1'], metadata: ['id'])
        def jsonQuery = " {\"type\" : \"update\", \"database\" :\"test\", \"table\":\"user1\", \"data\": {\"id\":\"1\"}}"

        QueryParser queryParser = new QueryParser(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(jsonQuery.getBytes()))))
        def actualQuery = queryParser.parseQuery()

        assertEquals(expectedQuery.table, actualQuery.table)
        assertEquals(expectedQuery.dataBase, actualQuery.dataBase)
        assertEquals(expectedQuery.type, actualQuery.type)
        expectedQuery.data.each { assertNotNull(actualQuery.data.remove(it.key)) }
        expectedQuery.metadata.each { assertTrue(actualQuery.metadata.remove(it)) }

    }

    @Test
    void parseDeleteTest() {
        Query expectedQuery = new Query(type: QueryType.DELETE, dataBase: 'test', table: 'user1')
        def jsonQuery = " {\"type\" : \"delete\", \"database\" :\"test\", \"table\":\"user1\"}"

        QueryParser queryParser = new QueryParser(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(jsonQuery.getBytes()))))
        def actualQuery = queryParser.parseQuery()

        assertEquals(expectedQuery, actualQuery)
    }

    @Test
    void parseCreateDatabaseTest() {
        Query expectedQuery = new Query(type: QueryType.CREATE_DATABASE, dataBase: 'test')
        def jsonQuery = " {\"type\" : \"create_database\", \"database\" :\"test\"}"

        QueryParser queryParser = new QueryParser(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(jsonQuery.getBytes()))))
        def actualQuery = queryParser.parseQuery()

        assertEquals(expectedQuery, actualQuery)

    }

    @Test
    void parseDropDataBaseTest() {
        Query expectedQuery = new Query(type: QueryType.DROP_DATABASE, dataBase: 'test')
        def jsonQuery = " {\"type\" : \"drop_database\", \"database\" :\"test\"}"

        QueryParser queryParser = new QueryParser(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(jsonQuery.getBytes()))))
        def actualQuery = queryParser.parseQuery()

        assertEquals(expectedQuery, actualQuery)


    }

    @Test
    void parseCreateTableTest() {
        Query expectedQuery = new Query(type: QueryType.CREATE_TABLE, dataBase: 'test', table: 'user1')
        def jsonQuery = " {\"type\" : \"create_table\", \"database\" :\"test\", \"table\":\"user1\"}"

        QueryParser queryParser = new QueryParser(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(jsonQuery.getBytes()))))
        def actualQuery = queryParser.parseQuery()

        assertEquals(expectedQuery, actualQuery)
    }

    @Test
    void parseDropTableTest() {
        Query expectedQuery = new Query(type: QueryType.DROP_TABLE, dataBase: 'test', table: 'user1')
        def jsonQuery = " {\"type\" : \"drop_table\", \"database\" :\"test\", \"table\":\"user1\"}"

        QueryParser queryParser = new QueryParser(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(jsonQuery.getBytes()))))
        def actualQuery = queryParser.parseQuery()

        assertEquals(expectedQuery, actualQuery)

    }


}
