package com.dzytsiuk.dbserver.testdata

import com.dzytsiuk.dbserver.service.DBService

class TestExecutor {

    void setUpOrTearDown(String... queries) {
        queries.each {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(it.getBytes())
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
            def socket = [getInputStream: inputStream, getOutputStream: outputStream,
                          close: {inputStream.close(); outputStream.close()}] as Socket
            DBService dbService = new DBService(socket)
            dbService.executeQuery()
            inputStream.close()
            outputStream.close()
        }
    }
}
