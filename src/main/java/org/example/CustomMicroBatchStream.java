package org.example;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CustomMicroBatchStream implements MicroBatchStream, SupportsAdmissionControl {

    private  Map<String, String> options;
    private String checkpointLocation;

    public CustomMicroBatchStream(Map<String, String> options, String checkpointLocation) {
        this.options = options;
        this.checkpointLocation = checkpointLocation;
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        //保障offset一定后移  下面会调用planInputPartitions
        return new MockTimestampOffset(System.currentTimeMillis());
    }

    @Override
    public Offset latestOffset() {
        throw new UnsupportedOperationException(
                "latestOffset(Offset, ReadLimit) should be called instead of this method");
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        AtomicInteger partitionCounter = new AtomicInteger(0);
        //获取有多少个Executor,每个Executor分配1个Task（计算任务）
        List<String> executors = JavaConverters.seqAsJavaList(SparkContext.getOrCreate().getExecutorIds());
        if (executors.isEmpty()) {
            executors = Arrays.asList("driver");
        }
        return executors.stream().map(
                ex -> new MockInputPartition(partitionCounter.getAndIncrement()))
                .collect(Collectors.toList()).toArray(new MockInputPartition[]{});
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        //每个Executor获得一个Task，执行Task，拉取数据
        return partition -> new MockInputPartitionReader(options);
    }

    @Override
    public Offset initialOffset() {
        //初始offset用-1表示
        return new MockTimestampOffset(-1);
    }

    @Override
    public Offset deserializeOffset(String json) {
        //反序列化成时间戳
        return new MockTimestampOffset(json);
    }

    @Override
    public void commit(Offset end) {

    }

    @Override
    public void stop() {
    }


}
