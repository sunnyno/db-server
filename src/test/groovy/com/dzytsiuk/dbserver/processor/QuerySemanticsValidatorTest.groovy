package com.dzytsiuk.dbserver.processor

import com.dzytsiuk.dbserver.entity.Query
import com.dzytsiuk.dbserver.entity.QueryType
import com.dzytsiuk.dbserver.processor.validator.QuerySemanticsValidator
import com.dzytsiuk.dbserver.processor.validator.StatusCode
import com.dzytsiuk.dbserver.testdata.TestDataProvider
import org.junit.Test


import static org.junit.Assert.*

class QuerySemanticsValidatorTest {
    private static final TestDataProvider QUERY_EXECUTOR = new TestDataProvider()

    @Test
    void validateInsertTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}",
                "{\"type\" : \"create_table\", \"database\" :\"test\", \"table\":\"user\", \"metadata\":[\"id\", \"name\"]}")
        Query query = new Query(type: QueryType.INSERT, dataBase: 'test', table: 'user',
                data: ['id': '1', 'name': 'test'], metadata: ['id', 'name'])
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateInsert(query)
        assertEquals(StatusCode.SUCCESS, actualResult)
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_table\", \"database\" :\"test\", \"table\":\"user\"}",
                "{\"type\" : \"drop_database\", \"database\" :\"test\"}")
    }

    @Test
    void validateSelectTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}",
                "{\"type\" : \"create_table\", \"database\" :\"test\", \"table\":\"user\", \"metadata\":[\"id\", \"name\"]}")
        Query query = new Query(type: QueryType.SELECT, dataBase: 'test', table: 'user')
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateSelect(query)
        assertEquals(StatusCode.SUCCESS, actualResult)
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_table\", \"database\" :\"test\", \"table\":\"user\"}",
                "{\"type\" : \"drop_database\", \"database\" :\"test\"}")
    }

    @Test
    void validateUpdateTest() {
        Query query = new Query(type: QueryType.UPDATE, dataBase: 'test', table: 'user1', data: ['id': '1'], metadata: ['id'])
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateUpdate(query)
        assertEquals(StatusCode.DATABASE_DOES_NOT_EXIST, actualResult)
    }


    @Test
    void validateDeleteTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}")
        Query query = new Query(type: QueryType.DELETE, dataBase: 'test', table: 'user')
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateDelete(query)
        assertEquals(StatusCode.TABLE_DOES_NOT_EXIST, actualResult)
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_database\", \"database\" :\"test\"}")
    }


    @Test
    void validateCreateTableTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}")
        Query query = new Query(type: QueryType.CREATE_TABLE, dataBase: 'test', table: 'user')
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateCreateTable(query)
        assertEquals(StatusCode.SUCCESS, actualResult)
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_database\", \"database\" :\"test\"}")
    }

    @Test
    void validateDropTableTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}",
                "{\"type\" : \"create_table\", \"database\" :\"test\", \"table\":\"user\", \"metadata\":[\"id\", \"name\"]}")
        Query query = new Query(type: QueryType.DROP_TABLE, dataBase: 'test', table: 'user')
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateDropTable(query)
        assertEquals(StatusCode.SUCCESS, actualResult)
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_table\", \"database\" :\"test\", \"table\":\"user\"}",
                "{\"type\" : \"drop_database\", \"database\" :\"test\"}")
    }


    @Test
    void validateCreateDatabaseTest() {
        Query query = new Query(type: QueryType.CREATE_DATABASE, dataBase: 'test')
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateCreateDatabase(query)
        assertEquals(StatusCode.SUCCESS, actualResult)
    }

    @Test
    void validateDropDatabaseTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}")
        Query query = new Query(type: QueryType.DROP_DATABASE, dataBase: 'test')
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateDropDatabase(query)
        assertEquals(StatusCode.SUCCESS, actualResult)
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_database\", \"database\" :\"test\"}")
    }
}
