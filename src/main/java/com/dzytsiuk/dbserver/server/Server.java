package com.dzytsiuk.dbserver.server;

import com.dzytsiuk.dbserver.service.DBService;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Server {
    private static final Executor EXECUTOR =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
    public static final String DB_STORAGE = "src/main/resources/database/";
    public static final String METADATA_XML_SUFFIX = "-metadata.xml";
    public static final String DATA_XML_SUFFIX = "-data.xml";


    private int port;

    public void start() {
        try {
            final ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("----> Server listening on port " + port);
            while (true) {

                try {

                    Socket socket = serverSocket.accept();
                    System.out.println("connected");
                    DBService dbService = new DBService(socket);
                    EXECUTOR.execute(dbService);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }


        } catch (IOException e) {

            e.printStackTrace();
        }
    }

    public void setPort(int port) {
        this.port = port;
    }
}
