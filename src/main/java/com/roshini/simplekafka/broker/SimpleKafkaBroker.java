package com.roshini.simplekafka.broker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SimpleKafkaBroker {

    private final int port;

    public SimpleKafkaBroker(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Broker listening on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("New client connected: " + clientSocket.getInetAddress());
        }
    }

    public static void main(String[] args) throws IOException {
        SimpleKafkaBroker broker = new SimpleKafkaBroker(9092);
        broker.start();
    }

}