package com.dzytsiuk.dbserver.service;

import com.dzytsiuk.dbserver.entity.Query;
import com.dzytsiuk.dbserver.executor.QueryExecutor;
import com.dzytsiuk.dbserver.executor.QueryParser;
import com.dzytsiuk.dbserver.executor.validator.QuerySemanticsValidator;
import com.dzytsiuk.dbserver.executor.ResultWriter;

import java.io.*;
import java.net.Socket;


public class DBService implements Runnable {

    private Socket socket;

    public DBService(Socket socket) {
        this.socket = socket;
    }


    public void executeQuery() throws Exception {

        InputStream inputStream = new BufferedInputStream(socket.getInputStream());
        OutputStream outputStream = new BufferedOutputStream(socket.getOutputStream());

        try (ResultWriter resultWriter = new ResultWriter(outputStream)) {
            System.out.println("execute");
            try {
                QueryParser queryParser = new QueryParser(inputStream);
                Query query = queryParser.parseQuery();

                QuerySemanticsValidator querySemanticsValidator = new QuerySemanticsValidator();

                QueryExecutor queryExecutor = new QueryExecutor(query);


                switch (query.getType()) {
                    case SELECT: {
                        String validationMessage = querySemanticsValidator.validateSelect(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.select());
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case INSERT: {
                        String validationMessage = querySemanticsValidator.validateInsert(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.insert());
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case DELETE: {
                        String validationMessage = querySemanticsValidator.validateDelete(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.delete());
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case UPDATE: {
                        String validationMessage = querySemanticsValidator.validateUpdate(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.update());
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case CREATE_TABLE: {
                        String validationMessage = querySemanticsValidator.validateCreateTable(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.createTable());
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case DROP_TABLE: {
                        String validationMessage = querySemanticsValidator.validateDropTable(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.dropTable());
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case CREATE_DATABASE: {
                        String validationMessage = querySemanticsValidator.validateCreateDatabase(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.createDatabase());
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }

                        break;
                    }
                    case DROP_DATABASE: {
                        String validationMessage = querySemanticsValidator.validateDropDatabase(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.dropDatabase());
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case ERROR: {
                            resultWriter.writeResult("Invalid query type");

                        break;
                    }
                }

            } catch (Exception e) {
                resultWriter.writeResult(e.getMessage());
            }
        }

    }


    @Override
    public void run() {
        try {

            while (!socket.isClosed()) {
                executeQuery();
            }

        } catch (Exception e) {
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }
}
