package com.dzytsiuk.dbserver.server;

import com.dzytsiuk.dbserver.service.DBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Server {
    public static final String DB_STORAGE = "database/";
    public static final String METADATA_XML_SUFFIX = "-metadata.xml";
    public static final String DATA_XML_SUFFIX = "-data.xml";
    private static final Logger logger = LoggerFactory.getLogger(DBService.class);


    private int port;
    private int threadCount;

    public void start() {
        final Executor EXECUTOR = Executors.newFixedThreadPool(threadCount);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            logger.info("----> Server listening on port {}", port);
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    logger.info("Connected");
                    DBService dbService = new DBService(socket);
                    EXECUTOR.execute(dbService);
                } catch (IOException e) {
                    logger.warn("Connection closed", e);
                }
            }
        } catch (IOException e) {
            logger.error("Server error", e);
        }
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
}
