package com.dzytsiuk.dbserver.executor;

import com.dzytsiuk.dbserver.exception.QueryExecuteException;

import java.io.*;
import java.nio.file.Files;

public class ResultWriter implements AutoCloseable {

    private static final int SUCCESS_RESULT = 1;
    private static final int FAILURE_RESULT = 0;
    private static final String ESCAPE_CHAR = "\\";
    private static final String newLine = System.getProperty("line.separator");
    private OutputStream outputStream;

    public ResultWriter(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void writeResult(File file) {

        try {
            Files.copy(file.toPath(), outputStream);

            outputStream.write(newLine.getBytes());
            outputStream.write(ESCAPE_CHAR.getBytes());
            outputStream.write(newLine.getBytes());
            outputStream.flush();
        } catch (IOException e) {
            throw new QueryExecuteException("Unable to fetch result", e);
        }

    }

    public void writeResult(int result) {
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
            bufferedWriter.write(String.valueOf(result));
            bufferedWriter.newLine();
            bufferedWriter.write(ESCAPE_CHAR);
            bufferedWriter.newLine();
            bufferedWriter.flush();
        } catch (IOException e) {
            throw new QueryExecuteException("Unable to fetch result", e);
        }

    }

    public void writeResult(boolean result) {
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
            bufferedWriter.write(String.valueOf(result ? SUCCESS_RESULT : FAILURE_RESULT));
            bufferedWriter.newLine();
            bufferedWriter.write(ESCAPE_CHAR);
            bufferedWriter.newLine();
            bufferedWriter.flush();
        } catch (IOException e) {
            throw new QueryExecuteException("Unable to fetch result", e);
        }

    }

    public void writeResult(String message) {
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
            bufferedWriter.write(message);
            bufferedWriter.newLine();
            bufferedWriter.write(ESCAPE_CHAR);
            bufferedWriter.newLine();
            bufferedWriter.flush();
        } catch (IOException e) {
            throw new QueryExecuteException("Unable to fetch result", e);
        }
    }

    @Override
    public void close() throws IOException {

        outputStream.flush();
    }
}
