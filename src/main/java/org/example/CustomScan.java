package org.example;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class CustomScan implements Scan {
    private Map<String, String> options;

    public CustomScan(Map<String, String> options) {
        this.options=options;
    }

    @Override
    public StructType readSchema() {
        return null;
    }

    @Override
    public String description() {
        return Scan.super.description();
    }

    @Override
    public Batch toBatch() {
        return Scan.super.toBatch();
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return new CustomMicroBatchStream(options,checkpointLocation);
    }

    @Override
    public ContinuousStream toContinuousStream(String checkpointLocation) {
        return Scan.super.toContinuousStream(checkpointLocation);
    }
}
