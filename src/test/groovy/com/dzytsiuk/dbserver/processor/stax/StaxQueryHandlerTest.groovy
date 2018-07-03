package com.dzytsiuk.dbserver.processor.stax

import com.dzytsiuk.dbserver.entity.Query
import com.dzytsiuk.dbserver.entity.QueryType
import org.junit.Test

import static org.junit.Assert.*

class StaxQueryHandlerTest {

    @Test
    void appendFirstDataElementTest() {
        File table = new File("test-database", 'user-data.xml')
        table.createNewFile();
        Query query = new Query(type: QueryType.INSERT, dataBase: 'test', table: 'user1', data: ['id': '1', 'name': 'test'], metadata: ['id', 'name'])

        StaxQueryHandler queryHandler = new StaxQueryHandler()
        queryHandler.appendFirstDataElement(table, query)

        assertTrue(table.size() > 0)
        table.delete()

    }

    @Test
    void appendDataTest() {
        File table = new File("test-database", 'user-data.xml')
        table.createNewFile();
        Query query = new Query(type: QueryType.INSERT, dataBase: 'test', table: 'user1', data: ['id': '1', 'name': 'test'], metadata: ['id', 'name'])
        StaxQueryHandler queryHandler = new StaxQueryHandler()
        queryHandler.appendFirstDataElement(table, query)
        def sizeBefore = table.size()
        queryHandler.appendData(table, query)
        def sizeAfter = table.size()
        assertTrue(sizeAfter > sizeBefore)
        table.delete()

    }

    @Test
    void appendMetaData() {
        File table = new File("test-database", 'user-metadata.xml')
        table.createNewFile();
        Query query = new Query(type: QueryType.INSERT, dataBase: 'test', table: 'user1', data: ['id': '1', 'name': 'test'], metadata: ['id', 'name'])
        StaxQueryHandler queryHandler = new StaxQueryHandler()
        queryHandler.appendMetaData(table, query)
        assertTrue(table.size() > 0)
        table.delete()

    }
}
