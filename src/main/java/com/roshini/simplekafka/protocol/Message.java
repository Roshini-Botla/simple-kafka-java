package com.roshini.simplekafka.protocol;

public class Message {
    private final String topic;
    private final int partition;
    private final String key;
    private final byte[] value;
    private long offset;
    private final long timestamp;

    public Message(String topic, int partition, String key, byte[] value, long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.offset = -1;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTimestamp() {
        return timestamp;
    }
}