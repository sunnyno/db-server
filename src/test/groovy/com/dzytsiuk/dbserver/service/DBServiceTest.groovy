package com.dzytsiuk.dbserver.service

import com.dzytsiuk.dbserver.testdata.TestExecutor
import org.junit.Test

import static org.junit.Assert.*

class DBServiceTest {
    static final TestExecutor QUERY_EXECUTOR = new TestExecutor()
    static final String ESCAPE_CHAR = "\\"

    @Test
    void executeCreateDatabaseTest() {

        def input = "{\"type\" : \"create_database\", \"database\" :\"test\"}"
        def expectedOutput = "1"

        ByteArrayInputStream inputStream = new ByteArrayInputStream(input.getBytes())
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        def socket = [getInputStream: inputStream, getOutputStream: outputStream,
                      close         : { inputStream.close(); outputStream.close() }] as Socket

        DBService dbService = new DBService(socket)
        dbService.executeQuery(socket.getInputStream(), socket.getOutputStream())
        String actualOutput = new String(outputStream.toByteArray())
        assertEquals(expectedOutput.trim(), actualOutput.replace(ESCAPE_CHAR, "").trim())

        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_database\", \"database\" :\"test\"}")

    }


    @Test
    void executeCreateTableTest() {

        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}")


        def input = "{\"type\" : \"create_table\", \"database\" :\"test\", \"table\":\"user\", \"metadata\":[\"id\", \"name\"]}"
        def expectedOutput = "1"

        ByteArrayInputStream inputStream = new ByteArrayInputStream(input.getBytes())
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        def socket = [getInputStream: inputStream, getOutputStream: outputStream,
                      close         : { inputStream.close(); outputStream.close() }] as Socket

        DBService dbService = new DBService(socket)
        dbService.executeQuery(socket.getInputStream(), socket.getOutputStream())
        String actualOutput = new String(outputStream.toByteArray())
        assertEquals(expectedOutput.trim(), actualOutput.replace(ESCAPE_CHAR, "").trim())

        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_table\", \"database\" :\"test\", \"table\":\"user\"}",
                "{\"type\" : \"drop_database\", \"database\" :\"test\"}")

    }

    @Test
    void executeInsertTest() {

        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}",
                "{\"type\" : \"create_table\", \"database\" :\"test\", \"table\":\"user\", \"metadata\":[\"id\", \"name\"]}")

        //test insert
        def input = "{\"type\" : \"insert\", \"database\" :\"test\", \"table\":\"user\", \"data\":{\"id\":\"1\", \"name\":\"John\"}}"
        def expectedOutput = "1"

        ByteArrayInputStream inputStream = new ByteArrayInputStream(input.getBytes())
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        def socket = [getInputStream: inputStream, getOutputStream: outputStream,
                      close         : { inputStream.close(); outputStream.close() }] as Socket

        DBService dbService = new DBService(socket)
        dbService.executeQuery(socket.getInputStream(), socket.getOutputStream())
        String actualOutput = new String(outputStream.toByteArray())
        assertEquals(expectedOutput.trim(), actualOutput.replace(ESCAPE_CHAR, "").trim())


        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_table\", \"database\" :\"test\", \"table\":\"user\"}",
                "{\"type\" : \"drop_database\", \"database\" :\"test\"}")

    }

    @Test
    void executeSelectTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}",
                "{\"type\" : \"create_table\", \"database\" :\"test\", \"table\":\"user\", \"metadata\":[\"id\", \"name\"]}",
                "{\"type\" : \"insert\", \"database\" :\"test\", \"table\":\"user\", \"data\":{\"id\":\"1\", \"name\":\"John\"}}")


        def input = "{\"type\" : \"select\", \"database\" :\"test\", \"table\":\"user\"}"

        def expectedOutput = "<?xml version=\"1.0\" ?><test><user><name>John</name><id>1</id></user></test>"
        ByteArrayInputStream inputStream = new ByteArrayInputStream(input.getBytes())
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        def socket = [getInputStream: inputStream, getOutputStream: outputStream,
                      close         : { inputStream.close(); outputStream.close() }] as Socket

        DBService dbService = new DBService(socket)
        dbService.executeQuery(socket.getInputStream(), socket.getOutputStream())
        String actualOutput = new String(outputStream.toByteArray())

        assertEquals(expectedOutput.trim(), actualOutput.replace(ESCAPE_CHAR, "").trim())


        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_table\", \"database\" :\"test\", \"table\":\"user\"}",
                "{\"type\" : \"drop_database\", \"database\" :\"test\"}")


    }

    @Test
    void executeDropTableTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}",
                "{\"type\" : \"create_table\", \"database\" :\"test\", \"table\":\"user\", \"metadata\":[\"id\", \"name\"]}")


        def input = "{\"type\" : \"drop_table\", \"database\" :\"test\", \"table\":\"user\"}"
        def expectedOutput = "1"

        ByteArrayInputStream inputStream = new ByteArrayInputStream(input.getBytes())
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        def socket = [getInputStream: inputStream, getOutputStream: outputStream,
                      close         : { inputStream.close(); outputStream.close() }] as Socket

        DBService dbService = new DBService(socket)
        dbService.executeQuery(socket.getInputStream(), socket.getOutputStream())
        String actualOutput = new String(outputStream.toByteArray())
        assertEquals(expectedOutput.trim(), actualOutput.replace(ESCAPE_CHAR, "").trim())

        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"drop_database\", \"database\" :\"test\"}")

    }

    @Test
    void executeDropDatabaseTest() {
        QUERY_EXECUTOR.setUpOrTearDown("{\"type\" : \"create_database\", \"database\" :\"test\"}")
        def input = "{\"type\" : \"drop_database\", \"database\" :\"test\"}"
        def expectedOutput = "1"

        ByteArrayInputStream inputStream = new ByteArrayInputStream(input.getBytes())
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        def socket = [getInputStream: inputStream, getOutputStream: outputStream,
                      close         : { inputStream.close(); outputStream.close() }] as Socket

        DBService dbService = new DBService(socket)
        dbService.executeQuery(socket.getInputStream(), socket.getOutputStream())
        String actualOutput = new String(outputStream.toByteArray())
        assertEquals(expectedOutput.trim(), actualOutput.replace(ESCAPE_CHAR, "").trim())
    }


}
