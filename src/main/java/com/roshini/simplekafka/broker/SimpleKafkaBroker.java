package com.roshini.simplekafka.broker;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import com.roshini.simplekafka.protocol.Message;
import com.roshini.simplekafka.protocol.MessageSerializer;

import com.roshini.simplekafka.storage.MessageLog;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleKafkaBroker {

    private final int port;
    private final Map<String, MessageLog> logs = new ConcurrentHashMap<>();

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

            MessageLog log = getOrCreateLog(message.getTopic(), message.getPartition());
            long assignedOffset = log.append(message);

            System.out.println("Stored message -> Topic: " + message.getTopic()
                    + ", Partition: " + message.getPartition()
                    + ", Offset: " + assignedOffset
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

    private MessageLog getOrCreateLog(String topic, int partition) throws IOException {
        String key = topic + "-" + partition;
        return logs.computeIfAbsent(key, k -> {
            try {
                return new MessageLog(key + ".log");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

}