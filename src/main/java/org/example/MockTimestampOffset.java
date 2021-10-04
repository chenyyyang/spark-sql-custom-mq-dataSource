package org.example;

import org.apache.spark.sql.connector.read.streaming.Offset;

public class MockTimestampOffset extends Offset {

    private long timestamp;
    public MockTimestampOffset(long timestamp) {
        this.timestamp=timestamp;
    }

    public MockTimestampOffset(String timestamp) {
        this.timestamp=Long.valueOf(timestamp);
    }

    @Override
    public String json() {
        return String.valueOf(timestamp);
    }
}
