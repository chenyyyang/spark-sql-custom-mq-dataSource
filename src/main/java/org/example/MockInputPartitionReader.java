package org.example;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import scala.collection.JavaConverters;
import scala.collection.mutable.Seq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;

public class MockInputPartitionReader implements PartitionReader<InternalRow> {
    public MockInputPartitionReader(Map<String, String> options) {
    }

    @Override
    public boolean next() throws IOException {
        return true;
    }

    @Override
    public InternalRow get() {
        String randomStr = "hello:"+ LocalDateTime.now();
        Seq seq = JavaConverters.asScalaBuffer(Arrays.asList(randomStr.getBytes(StandardCharsets.UTF_8))).seq();
        return InternalRow.apply(seq);
    }

    @Override
    public void close() throws IOException {

    }
}
