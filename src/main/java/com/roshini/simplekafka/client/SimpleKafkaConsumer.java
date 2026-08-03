package com.roshini.simplekafka.client;

import com.roshini.simplekafka.protocol.Message;
import com.roshini.simplekafka.protocol.MessageSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class SimpleKafkaConsumer {

    public static void main(String[] args) throws IOException {
        String host = "localhost";
        int port = 9092;

        String topic = "orders";
        int partition = 0;
        long offset = 0;

        try (Socket socket = new Socket(host, port)) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeByte(1);
            out.writeUTF(topic);
            out.writeInt(partition);
            out.writeLong(offset);
            out.flush();

            boolean found = in.readBoolean();

            if (!found) {
                System.out.println("No message found at offset " + offset);
                return;
            }

            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data);

            Message message = MessageSerializer.deserialize(data);

            System.out.println("Consumed message -> Topic: " + message.getTopic()
                    + ", Partition: " + message.getPartition()
                    + ", Offset: " + message.getOffset()
                    + ", Key: " + message.getKey()
                    + ", Value: " + new String(message.getValue()));
        }
    }

}