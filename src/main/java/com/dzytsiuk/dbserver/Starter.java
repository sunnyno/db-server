package com.dzytsiuk.dbserver;

import com.dzytsiuk.dbserver.server.Server;

public class Starter {

    public static void main(String[] args) {
        Server server = new Server();
        server.setPort(3000);

        server.start();
    }
}
