package com.roshini.simplekafka.protocol;

import java.io.IOException;
import java.util.Arrays;

public class MessageTest {

    public static void main(String[] args) throws IOException {
        Message original = new Message("orders", 0, "user123", "Hello Kafka".getBytes(), System.currentTimeMillis());
        original.setOffset(42);

        byte[] serialized = MessageSerializer.serialize(original);
        System.out.println("Serialized size: " + serialized.length + " bytes");

        Message result = MessageSerializer.deserialize(serialized);

        System.out.println("Topic: " + result.getTopic());
        System.out.println("Partition: " + result.getPartition());
        System.out.println("Key: " + result.getKey());
        System.out.println("Value: " + new String(result.getValue()));
        System.out.println("Offset: " + result.getOffset());
        System.out.println("Timestamp: " + result.getTimestamp());

        boolean valuesMatch = Arrays.equals(original.getValue(), result.getValue());
        boolean allMatch = original.getTopic().equals(result.getTopic())
                && original.getPartition() == result.getPartition()
                && original.getKey().equals(result.getKey())
                && valuesMatch
                && original.getOffset() == result.getOffset()
                && original.getTimestamp() == result.getTimestamp();

        System.out.println("Round-trip successful: " + allMatch);
    }

}