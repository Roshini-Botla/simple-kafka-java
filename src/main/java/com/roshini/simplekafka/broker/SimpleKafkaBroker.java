package com.roshini.simplekafka.broker;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import com.roshini.simplekafka.protocol.Message;
import com.roshini.simplekafka.protocol.MessageSerializer;

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
            handleClient(clientSocket);
        }
    }

    private void handleClient(Socket clientSocket) {
        try {
            DataInputStream in = new DataInputStream(clientSocket.getInputStream());

            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data);

            Message message = MessageSerializer.deserialize(data);

            System.out.println("Received message -> Topic: " + message.getTopic()
                    + ", Key: " + message.getKey()
                    + ", Value: " + new String(message.getValue()));

            clientSocket.close();
        } catch (IOException e) {
            System.out.println("Error handling client: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        SimpleKafkaBroker broker = new SimpleKafkaBroker(9092);
        broker.start();
    }

}