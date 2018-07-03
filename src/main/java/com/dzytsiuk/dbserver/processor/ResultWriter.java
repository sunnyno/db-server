package com.dzytsiuk.dbserver.processor;

import com.dzytsiuk.dbserver.exception.QueryExecuteException;
import com.dzytsiuk.dbserver.processor.query.DefaultQuery;
import com.dzytsiuk.dbserver.processor.validator.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;

public class ResultWriter implements AutoCloseable {

    private static final int SUCCESS_RESULT = 0;
    private static final int FAILURE_RESULT = 1;
    private static final byte[] ESCAPE_CHAR = "\\\n".getBytes();
    private OutputStream outputStream;
    private Logger logger = LoggerFactory.getLogger(DefaultQuery.class);

    public ResultWriter(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void writeResult(File file) {

        try {
            Files.copy(file.toPath(), outputStream);
            outputStream.write(ESCAPE_CHAR);
            outputStream.flush();
            logger.info("Result sent");
        } catch (IOException e) {
            logger.error("Unable to fetch result", e);
            throw new QueryExecuteException("Unable to fetch result", e);
        }

    }

    public void writeResult(int result) {
        try {
            outputStream.write(String.valueOf(result).getBytes());
            outputStream.write(ESCAPE_CHAR);
            logger.info("Result {} sent", result);
        } catch (IOException e) {
            logger.error("Unable to fetch result", e);
            throw new QueryExecuteException("Unable to fetch result", e);
        }

    }

    public void writeResult(boolean result) {
        try {
            outputStream.write(String.valueOf(result ? SUCCESS_RESULT : FAILURE_RESULT).getBytes());
            outputStream.write(ESCAPE_CHAR);
            logger.info("Result {} sent", result);
        } catch (IOException e) {
            logger.error("Unable to fetch result", e);
            throw new QueryExecuteException("Unable to fetch result", e);
        }


    }

    public void writeResult(StatusCode message) {
        writeResult(message.getStatus());
    }

    public void writeResult(String message) {
        try {
            outputStream.write(message.getBytes());
            outputStream.write(ESCAPE_CHAR);
            logger.info("Result {} sent", message);
        } catch (IOException e) {
            logger.error("Unable to fetch result", e);
            throw new QueryExecuteException("Unable to fetch result", e);
        }
    }

    @Override
    public void close() throws IOException {
        outputStream.flush();
    }
}
