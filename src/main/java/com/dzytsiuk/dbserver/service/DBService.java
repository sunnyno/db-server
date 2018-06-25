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


    public void executeQuery(InputStream inputStream, OutputStream outputStream) throws IOException {

        try (ResultWriter resultWriter = new ResultWriter(outputStream)) {
            try {
                QueryParser queryParser = new QueryParser(inputStream);
                Query query = queryParser.parseQuery();

                QuerySemanticsValidator querySemanticsValidator = new QuerySemanticsValidator();

                QueryExecutor queryExecutor = new QueryExecutor();


                switch (query.getType()) {
                    case SELECT: {
                        String validationMessage = querySemanticsValidator.validateSelect(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.select(query));
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case INSERT: {
                        String validationMessage = querySemanticsValidator.validateInsert(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.insert(query));
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case DELETE: {
                        String validationMessage = querySemanticsValidator.validateDelete(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.delete(query));
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case UPDATE: {
                        String validationMessage = querySemanticsValidator.validateUpdate(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.update(query));
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case CREATE_TABLE: {
                        String validationMessage = querySemanticsValidator.validateCreateTable(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.createTable(query));
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case DROP_TABLE: {
                        String validationMessage = querySemanticsValidator.validateDropTable(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.dropTable(query));
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }
                        break;
                    }
                    case CREATE_DATABASE: {
                        String validationMessage = querySemanticsValidator.validateCreateDatabase(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.createDatabase(query));
                        } else {
                            resultWriter.writeResult(validationMessage);
                        }

                        break;
                    }
                    case DROP_DATABASE: {
                        String validationMessage = querySemanticsValidator.validateDropDatabase(query);
                        if (validationMessage == null) {
                            resultWriter.writeResult(queryExecutor.dropDatabase(query));
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
        try (BufferedInputStream inputStream = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream outputStream = new BufferedOutputStream(socket.getOutputStream())) {

            while (!socket.isClosed()) {
                executeQuery(inputStream, outputStream);
            }

        } catch (IOException e) {
            throw new RuntimeException("Error getting streams", e);
        } finally {
            //??
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
