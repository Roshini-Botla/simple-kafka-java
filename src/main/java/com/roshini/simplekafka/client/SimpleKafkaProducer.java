package com.roshini.simplekafka.client;

import com.roshini.simplekafka.protocol.Message;
import com.roshini.simplekafka.protocol.MessageSerializer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class SimpleKafkaProducer {

    public static void main(String[] args) throws IOException {
        String host = "localhost";
        int port = 9092;

        Message message = new Message("orders", 0, "user1", "Hello from producer!".getBytes(), System.currentTimeMillis());

        try (Socket socket = new Socket(host, port)) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            out.writeByte(0);

            byte[] data = MessageSerializer.serialize(message);

            out.writeInt(data.length);
            out.write(data);
            out.flush();

            System.out.println("Message sent successfully.");
        }
    }

}