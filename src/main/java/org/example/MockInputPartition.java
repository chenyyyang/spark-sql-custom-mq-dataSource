package org.example;

import org.apache.spark.sql.connector.read.InputPartition;

public class MockInputPartition implements InputPartition {

    private int partitionIndex;

    public MockInputPartition(int partitionIndex) {
        this.partitionIndex=partitionIndex;
    }
}
