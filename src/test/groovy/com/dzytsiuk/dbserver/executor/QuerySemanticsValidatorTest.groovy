package com.dzytsiuk.dbserver.executor

import com.dzytsiuk.dbserver.entity.Query
import com.dzytsiuk.dbserver.entity.QueryType
import com.dzytsiuk.dbserver.executor.validator.QuerySemanticsValidator
import com.dzytsiuk.dbserver.testdata.TestExecutor
import org.junit.Test


import static org.junit.Assert.*;

class QuerySemanticsValidatorTest {
    private static final TestExecutor QUERY_EXECUTOR = new TestExecutor()

    @Test
    void validateInsertTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}",
                "{\"type\" : \"create_table\", \"database\" :\"test\", \"table\":\"user\", \"metadata\":[\"id\", \"name\"]}")

        Query query = new Query(type: QueryType.INSERT, dataBase: 'test', table: 'user',
                data: ['id': '1', 'name': 'test'], metadata: ['id', 'name'])
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateInsert(query)

        assertNull(actualResult)

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


        assertNull(actualResult)

        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_table\", \"database\" :\"test\", \"table\":\"user\"}",
                "{\"type\" : \"drop_database\", \"database\" :\"test\"}")

    }

    @Test
    void validateUpdateTest() {

        Query query = new Query(type: QueryType.UPDATE, dataBase: 'test', table: 'user1',  data: ['id': '1'], metadata: ['id'])
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateUpdate(query)
        def expected = 'Database test does not exist'

        assertEquals(expected,actualResult)


    }


    @Test
    void validateDeleteTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}")

        Query query = new Query(type: QueryType.DELETE, dataBase: 'test', table: 'user')
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateDelete(query)
        def expected = 'Table user does not exist'

        assertEquals(expected,actualResult)

        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_database\", \"database\" :\"test\"}")
    }


    @Test
    void validateCreateTableTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}")

        Query query = new Query(type: QueryType.CREATE_TABLE, dataBase: 'test', table: 'user')
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateCreateTable(query)

        assertNull(actualResult)

        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_database\", \"database\" :\"test\"}")
    }

    @Test
    void validateDropTableTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}",
                "{\"type\" : \"create_table\", \"database\" :\"test\", \"table\":\"user\", \"metadata\":[\"id\", \"name\"]}")

        Query query = new Query(type: QueryType.DROP_TABLE, dataBase: 'test', table: 'user')
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateDropTable(query)

        assertNull(actualResult)

        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_table\", \"database\" :\"test\", \"table\":\"user\"}",
                "{\"type\" : \"drop_database\", \"database\" :\"test\"}")
    }


    @Test
    void validateCreateDatabaseTest() {

        Query query = new Query(type: QueryType.CREATE_DATABASE, dataBase: 'test')
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateCreateDatabase(query)

        assertNull(actualResult)

    }

    @Test
    void validateDropDatabaseTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}")

        Query query = new Query(type: QueryType.DROP_DATABASE, dataBase: 'test')
        QuerySemanticsValidator queryValidator = new QuerySemanticsValidator()
        def actualResult = queryValidator.validateDropDatabase(query)

        assertNull(actualResult)

        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_database\", \"database\" :\"test\"}")

    }
}
