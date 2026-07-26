package com.roshini.simplekafka.storage;

import com.roshini.simplekafka.protocol.Message;
import com.roshini.simplekafka.protocol.MessageSerializer;

import java.io.RandomAccessFile;
import java.io.IOException;

public class MessageLog {

    private final RandomAccessFile file;
    private long nextOffset;

    public MessageLog(String filePath) throws IOException {
        this.file = new RandomAccessFile(filePath, "rw");
        this.file.seek(file.length());
        this.nextOffset = 0;
    }

    public synchronized long append(Message message) throws IOException {
        message.setOffset(nextOffset);

        byte[] data = MessageSerializer.serialize(message);

        file.writeInt(data.length);
        file.write(data);

        long assignedOffset = nextOffset;
        nextOffset++;

        return assignedOffset;
    }

    public synchronized Message read(long offset) throws IOException {
        file.seek(0);
        long currentOffset = 0;

        while (file.getFilePointer() < file.length()) {
            int length = file.readInt();
            byte[] data = new byte[length];
            file.readFully(data);

            if (currentOffset == offset) {
                return MessageSerializer.deserialize(data);
            }

            currentOffset++;
        }

        return null;
    }

}