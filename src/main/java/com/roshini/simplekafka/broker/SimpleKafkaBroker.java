package com.roshini.simplekafka.broker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
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

            byte requestType = in.readByte();

            if (requestType == 0) {
                handleProduceRequest(in, clientSocket);
            } else if (requestType == 1) {
                handleConsumeRequest(in, clientSocket);
            } else {
                System.out.println("Unknown request type: " + requestType);
            }

            clientSocket.close();
        } catch (IOException e) {
            System.out.println("Error handling client: " + e.getMessage());
        }
    }

    private void handleProduceRequest(DataInputStream in, Socket clientSocket) throws IOException {
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
    }

    private void handleConsumeRequest(DataInputStream in, Socket clientSocket) throws IOException {
        String topic = in.readUTF();
        int partition = in.readInt();
        long offset = in.readLong();

        MessageLog log = getOrCreateLog(topic, partition);
        Message message = log.read(offset);

        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());

        if (message == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            byte[] data = MessageSerializer.serialize(message);
            out.writeInt(data.length);
            out.write(data);
        }

        out.flush();

        System.out.println("Consume request -> Topic: " + topic
                + ", Partition: " + partition
                + ", Offset: " + offset
                + ", Found: " + (message != null));
    }  

   public static void main(String[] args) throws IOException {
        int brokerId = 1;
        String host = "localhost";
        int port = 9092;

        SimpleKafkaBroker broker = new SimpleKafkaBroker(port);

        try {
            ZkRegistration zkRegistration = new ZkRegistration("localhost:2181");
            zkRegistration.registerBroker(brokerId, host, port);
        } catch (Exception e) {
            System.out.println("Failed to register with ZooKeeper: " + e.getMessage());
        }

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