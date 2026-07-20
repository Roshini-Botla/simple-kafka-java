package com.roshini.simplekafka.protocol;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class MessageSerializer {

    public static byte[] serialize(Message message) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteStream);

        out.writeUTF(message.getTopic());
        out.writeInt(message.getPartition());

        if (message.getKey() != null) {
            out.writeBoolean(true);
            out.writeUTF(message.getKey());
        } else {
            out.writeBoolean(false);
        }

        out.writeInt(message.getValue().length);
        out.write(message.getValue());

        out.writeLong(message.getOffset());
        out.writeLong(message.getTimestamp());

        out.flush();
        return byteStream.toByteArray();
    }

    public static Message deserialize(byte[] data) throws IOException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
        DataInputStream in = new DataInputStream(byteStream);

        String topic = in.readUTF();
        int partition = in.readInt();

        String key = null;
        boolean hasKey = in.readBoolean();
        if (hasKey) {
            key = in.readUTF();
        }

        int valueLength = in.readInt();
        byte[] value = new byte[valueLength];
        in.readFully(value);

        long offset = in.readLong();
        long timestamp = in.readLong();

        Message message = new Message(topic, partition, key, value, timestamp);
        message.setOffset(offset);

        return message;
    }

}