package com.roshini.simplekafka.storage;

import com.roshini.simplekafka.protocol.Message;

import java.io.IOException;

public class MessageLogTest {

    public static void main(String[] args) throws IOException {
        MessageLog log = new MessageLog("test-log.bin");

        Message msg1 = new Message("orders", 0, "user1", "First message".getBytes(), System.currentTimeMillis());
        Message msg2 = new Message("orders", 0, "user2", "Second message".getBytes(), System.currentTimeMillis());
        Message msg3 = new Message("orders", 0, "user3", "Third message".getBytes(), System.currentTimeMillis());

        long offset1 = log.append(msg1);
        long offset2 = log.append(msg2);
        long offset3 = log.append(msg3);

        System.out.println("Appended at offsets: " + offset1 + ", " + offset2 + ", " + offset3);

        Message readBack = log.read(1);
        System.out.println("Message at offset 1 -> Key: " + readBack.getKey() + ", Value: " + new String(readBack.getValue()));

        boolean correct = readBack.getKey().equals("user2") && new String(readBack.getValue()).equals("Second message");
        System.out.println("Read correct message: " + correct);
    }

}