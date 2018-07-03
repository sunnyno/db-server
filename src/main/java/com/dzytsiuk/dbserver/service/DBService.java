package com.dzytsiuk.dbserver.service;

import com.dzytsiuk.dbserver.entity.Query;
import com.dzytsiuk.dbserver.processor.QueryParser;
import com.dzytsiuk.dbserver.processor.ResultWriter;
import com.dzytsiuk.dbserver.processor.query.DefaultQuery;
import com.dzytsiuk.dbserver.processor.query.factory.QueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;


public class DBService implements Runnable {

    private Socket socket;
    private static final QueryFactory QUERY_FACTORY = QueryFactory.getInstance();
    private boolean isSocketClosed = false;
    private static final Logger logger = LoggerFactory.getLogger(DBService.class);


    public DBService(Socket socket) {
        this.socket = socket;
    }


    public void executeQuery(BufferedInputStream inputStream, OutputStream outputStream) throws IOException {

        try (ResultWriter resultWriter = new ResultWriter(outputStream)) {
            try {
                QueryParser queryParser = new QueryParser(inputStream);
                Query query = queryParser.parseQuery();
                DefaultQuery defaultQuery = QUERY_FACTORY.getDefaultQuery(query.getType());
                defaultQuery.execute(query, resultWriter);
            } catch (InterruptedException e) {
                isSocketClosed = true;
                logger.info("disconnected");
            } catch (Exception e) {
                if (!isSocketClosed) {
                    logger.error("Execution error ", e);
                    resultWriter.writeResult(e.getMessage());
                }
            }
        }
    }

    @Override
    public void run() {
        try (Socket mySocket = socket;
             BufferedOutputStream outputStream = new BufferedOutputStream(socket.getOutputStream());
             BufferedInputStream inputStream = new BufferedInputStream(socket.getInputStream())) {
            while (!isSocketClosed) {
                executeQuery(inputStream, outputStream);
            }
        } catch (IOException e) {
            logger.error("Socket exception", e);
        }
    }
}
