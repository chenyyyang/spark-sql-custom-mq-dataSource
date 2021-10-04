
> 本文基于 Spark 3.1.2 版本 （最新的stable版本）  
> 可能是全网第一篇中文 Spark 3.x Kafka数据源源码的博文了，纯手写，如有错误，望诸君指点一二

# 如何理解Spark Structured Streaming流处理

![image.png](https://p9-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/f2e7750b9c314f6bb05b1a8ad79beeac~tplv-k3u1fbpfcp-watermark.image?)

Structured Streaming是基于Spark SQL引擎处理无界流数据的模型。相比于基于RDD的Spark Streaming，Structured Streaming基于DataFrame DataSet API,推出了很多关于数据一致性和性能提升。

![image.png](https://p6-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/b4fc6d3f5c3b42f6b0e8eb72417e45b2~tplv-k3u1fbpfcp-watermark.image?)
就像这个图，Data stream数据流可以是Kafka 的一个Topic，KafkaProducer 不断的往这个Topic中生产数据，可以视为不停的为这个表添加行，且数据列是固定的schema，有key,value,topic,partition,offset,timestamp等字段（如下表）。Structured Streaming流处理则是间隔一段时间，把表中的数据查出来做计算，并将计算结果写入另外一张表（另外一个Topic），一次查询成为一个微批（MicroBatch）。

| key | value | topic | partition | offset |timestamp|
| --- | --- |--- |--- |--- |--- |
|  "key"|"hello world"|"kafka_topic"| 0 | 100 |1631623473|


# 跑个小Sample
接下来，我们用Java来实现一个实时统计Sample，流处理届的 HelloWorld-- WordCount，Kafka Producer不停的往 Topic 中写入各种单词，统计每个单词出现的次数并输出，这是非常典型的，GroupByKey的统计任务。
### 本机PC启动kafka Broker
```
//官网下载kafka和zookeeper，并解压...

//kafka启动前需要启动zk
$ bin/zookeeper-server-start.sh config/zookeeper.properties

//启动kafka，默认本机9092
$ bin/kafka-server-start.sh config/server.properties

//使用kafka自带脚本创建一个 名字叫quickstart-events的 topic
$ bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092

```

### 编写流计算代码

1.新建一个Java Maven工程

2.pom加入依赖
```
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>2.2.2</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.1.2</version>
        </dependency>
        
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_2.12</artifactId>
            <version>3.1.2</version>
        </dependency>
  

```

3.写一个main方法
```
public static void main(String[] args) throws Exception {

    SparkConf config = new SparkConf().setAppName("JavaStructuredKafkaWordCount").setMaster("local[1]");

    SparkContext sparkContext = new SparkContext(config);

    SparkSession spark = new  SparkSession(sparkContext);

    // Create DataSet representing the stream of input lines from kafka
    Dataset<String> lines = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "127.0.0.1:9092")
            .option("subscribe", "quickstart-events")
            .load()
            .selectExpr("CAST(value AS STRING)")
            .as(Encoders.STRING());

    // Generate running word count
    Dataset<org.apache.spark.sql.Row> wordCounts = lines.flatMap(
            (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
            Encoders.STRING()).groupBy("value").count();

    // Start running the query that prints the running counts to the console
    StreamingQuery query = wordCounts.writeStream()
            .outputMode("complete")
            .format("console")
            .start();

    query.awaitTermination();

}
```
4.启动的Kafka producer 往topic中生产单词...
```

//启动一个生产者,这样就可以在shell中给topic发消息
$ bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
//输入后回车
> hello owen
> hello lemon
> byebye
```

5.可以通过IDEA控制台查看每个单词出现的次数...
```

-------------------------------------------
Batch: 1
-------------------------------------------
+------+-----+
| value|count|
+------+-----+
| hello|    2|
|  owen|    1|
|byebye|    1|
| lemon|    1|
+------+-----+

```

# 微批流处理是如何从Kafka中读取数据的？

还有一些需要思考的问题
- Kafka Topic中的消息来一条就会写入InputTable吗？
- Structured Streaming + Kafka 如何保障数据不丢？
- Structured Streaming 没有用Kafka的消费者重平衡机制，如何分配消费者和partition的关系？
- Structured Streaming 如何实现 Kafka Offsets 管理？
- 自研MQ如何实现 最新的数据源接口，成为Spark的Source？
- Spark 输入输出源相关接口API V1 V2 多次迭代的原因是什么？


# 微批流处理中的数据源

![image.png](https://p9-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/198aa6e9d7704066b84a302514fd2455~tplv-k3u1fbpfcp-watermark.image) 

就像Mysql SQLlite等DB都要去实现 JDBC的标准接口
```
javax.sql.DataSource

```

Spark 与其他第三方组建进行数据交换，比如Kafka、Hdfs、文件等，也定义了一套数据源接口，通过SourceAPI 读取数据，通过SinkAPI写出数据。 

### 数据源API的迭代
- Spark 2.3.x 以前的版本，只有V1 API。主要的接口有[RelationProvider, DataSourceRegister, BaseRelation](https://github.com/apache/spark/blob/86cc907448f0102ad0c185e87fcc897d0a32707f/sql/core/src/main/scala/org/apache/spark/sql/sources/interfaces.scala)

- Spark 2.3.0带来了V2 API，这部分代码的作者（[Wenchen Fan](https://github.com/cloud-fan)）在 [视频](https://www.youtube.com/watch?v=9-eomYXVnvY&ab_channel=Databricks) 中介绍了这次改动，主要的改进是 并发读取数据、过滤器下推(filters push down)、加速聚合操作，增加Spark Data Encoding、数据统计和分片、支持Structured Streaming等
```
Data source interfaces in Spark 2.3.0
主要有以下的接口
-   StreamSourceProvider
-   Source
-   StreamSinkProvider
-   Sink
```


- V2 API 经历了Spark 2.4.0的改动后

- Spark 3.0.0 带来了全新的数据源API，并保留了V1 V2的接口和代码（这也导致代码量看起来有点多）
```
Data Source interfaces in Spark 3.0.0
主要有以下的接口
-   TableProvider
-   Table
-   ScanBuilder
-   Scan
-   MicroBatchStream
-   InputPartition
-   PartitionReaderFactory
-   PartitionReader
```

Structured Streaming 一般与 Kafka 集成进行微批处理。  
那么来看一下 Kafka 数据源是如何实现上述 数据源接口的。


# KafkaSourceProvider extends SimpleTableProvider

```
private[kafka010] class KafkaSourceProvider 
    extends DataSourceRegister //V1 接口
    with StreamSourceProvider // V2 接口
    with StreamSinkProvider  // V2 接口
    with RelationProvider  // V1  接口
    with CreatableRelationProvider  // V1 接口
    with SimpleTableProvider  //Spark 3.x 数据源接口
    with Logging {
```
所有的数据源的入口都是 XXXSourceProvider，比如上面的KafkaSourceProvider，如上注释，V1 V2的代码先不关注了。  
只需要关注SimpleTableProvider 这个接口和这个接口唯一的getTable()方法。

这里将Kafka也抽象成是一个数据库，就像Mysql，一个Topic就表示一个表（Table）,Topic中新增加的数据就像是对这个表源源不断的增加行，Spark从InputTable读取数据后，进行计算，然后写入OutputTable，也就是写入另外一个Topic。因此将数据源表的入口称为 TableProvider  

```
private[kafka010] class KafkaSourceProvider
    extends SimpleTableProvider // 1.Spark 3.x 数据源接口
    with Logging {


//2.getTable()方法，传入用户参数options，new出KafkaTable对象返回
  override def getTable(options: CaseInsensitiveStringMap): KafkaTable = {
     // 3. includeHeaders：是否包含Kafka 消息header信息
    val includeHeaders = options.getBoolean(INCLUDE_HEADERS, false)
    
    new KafkaTable(includeHeaders)
  }
  
```



# KafkaTable extends Table
Structured Streaming结构化流处理，这个接口告诉用户，把Kafka的Topic抽象成一个有固定字段的表。  
这个表有key,value,topic,partition,offset,timestamp,timestampType等固定的字段  
对 Kafka的表 可以进行微批读 和 写入功能。
```
class KafkaTable(includeHeaders: Boolean) extends Table with SupportsRead with SupportsWrite {

//1. 结构化数据可以当成一张表，这里就定义了这个表有哪些字段
/** StructField("key", BinaryType), 1.1 字段key
    StructField("value", BinaryType), 1.2 字段value
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", TimestampType),
    StructField("timestampType", IntegerType),
    StructField("headers", headersType) 可选 */
  override def schema(): StructType = KafkaRecordToRowConverter.kafkaSchema(includeHeaders)

//2.该方法返回表示该数据源支持哪些能力
// 对Kafka数据源，一般使用MICRO_BATCH_READ 微批读，STREAMING_WRITE 写能力
// 其他能力暂时可以忽略
  override def capabilities(): ju.Set[TableCapability] = {
    Set(BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, CONTINUOUS_READ, STREAMING_WRITE,
      ACCEPT_ANY_SCHEMA).asJava
  }

//3. new 出KafkaScan对象，返回，下文会继续说
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    () => new KafkaScan(options)

//4. new 出WriteBuilder对象返回，根据capabilities()方法中的BATCH_WRITE，STREAMING_WRITE，
// 这边对应有这两种能力 对应的处理对象，buildForBatch，buildForStreaming
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder with SupportsTruncate with SupportsStreamingUpdateAsAppend {     
      override def buildForBatch(): BatchWrite = {
        new KafkaBatchWrite(topic, producerParams, inputSchema)
      }
      override def buildForStreaming(): StreamingWrite = {
        new KafkaStreamingWrite(topic, producerParams, inputSchema)
      }
    }
  }
}
```

# KafkaScan extends Scan
如TableCapability中返回的三种读能力，BATCH_READ，MICRO_BATCH_READ，CONTINUOUS_READ，这里对应有三个方法，本文会选择最常用的微批读取来继续下文内容，其他两种读取方式，感兴趣的同学可以自行了解。需要注意的是，目前的这些操作都是在Driver进程中完成的，还未真实的开始读取Kafka中的数据。
```
class KafkaScan(options: CaseInsensitiveStringMap) extends Scan {

    override def toBatch(): Batch = {
     //1.省略这部分功能和代码，批处理
      new KafkaBatch(
        strategy(caseInsensitiveOptions),
        caseInsensitiveOptions,
        specifiedKafkaParams,
        failOnDataLoss(caseInsensitiveOptions),
        startingRelationOffsets,
        endingRelationOffsets,
        includeHeaders)
    }
//2. 和kafka集成后常用的 微批处理
    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      val caseInsensitiveOptions = CaseInsensitiveMap(options.asScala.toMap)
     
     // 3.每个流计算任务，生成唯一的groupId
     //这也是为后面管理Kafka offset和partition分配铺垫，后面会详细说
      val uniqueGroupId = streamingUniqueGroupId(caseInsensitiveOptions, checkpointLocation)

       //4. 解析出用户指定的kafka参数，比如kafka broker的鉴权账号密码等
       // 可以参考官方文档，options中以kafka. 开头的参数
      val specifiedKafkaParams = convertToSpecifiedParams(caseInsensitiveOptions)

        //5. 根据用户的设置，选择开始读取的位点，
        //从earliest开始读 或者从指定的offset/timestamp开始读取
      val startingStreamOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(
        caseInsensitiveOptions, STARTING_OFFSETS_BY_TIMESTAMP_OPTION_KEY,
        STARTING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)
    
    //4.创建一个KafkaOffsetReader对象，其实是对Kafka Consumer/ Kafka AdminClient的封装，主要用于在Driver进程中读取 Offset
      val kafkaOffsetReader = KafkaOffsetReader.build(
        strategy(caseInsensitiveOptions),
        kafkaParamsForDriver(specifiedKafkaParams),
        caseInsensitiveOptions,
        driverGroupIdPrefix = s"$uniqueGroupId-driver")

      //5. 跟前面的差不多，这里构造出一个KafkaMicroBatchStream对象，
      // 核心的逻辑都在这个类里面了
      new KafkaMicroBatchStream(
        kafkaOffsetReader,
        kafkaParamsForExecutors(specifiedKafkaParams, uniqueGroupId),
        options,
        checkpointLocation,
        startingStreamOffsets,
        failOnDataLoss(caseInsensitiveOptions))
    }

    override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
//省略这部分功能和代码，连续处理
      new KafkaContinuousStream(
        kafkaOffsetReader,
        kafkaParamsForExecutors(specifiedKafkaParams, uniqueGroupId),
        options,
        checkpointLocation,
        startingStreamOffsets,
        failOnDataLoss(caseInsensitiveOptions))
    }
}
```


# KafkaMicroBatchStream extends MicroBatchStream
### 前面那些都是new一些对象，做一些参数处理，核心逻辑都在这个类中
序号 1 2 3 也代表的代码的执行顺序

```
/**
 * A [[MicroBatchStream]] that reads data from Kafka.
 *
 * The [[KafkaSourceOffset]] is the custom [[Offset]] defined for this source that contains
 * a map of TopicPartition -> offset. Note that this offset is 1 + (available offset). For
 * example if the last record in a Kafka topic "t", partition 2 is offset 5, then
 * KafkaSourceOffset will contain TopicPartition("t", 2) -> 6. This is done keep it consistent
 * with the semantics of `KafkaConsumer.position()`.
 
KafkaSourceOffset这个对象是 Map<TopicPartition, Long>这样一个map，外面包了一层，这个对象可以json序列化和反序列化。
记录了一个Topic下所有partition的offset信息。

 * Zero data lost is not guaranteed when topics are deleted. If zero data lost is critical, the user
 * must make sure all messages in a topic have been processed when deleting a topic.
为了保障数据不丢失，请消费完数据后再删除topic
 *
 * There is a known issue caused by KAFKA-1894: the query using Kafka maybe cannot be stopped.
 * To avoid this issue, you should make sure stopping the query before stopping the Kafka brokers
 * and not use wrong broker addresses.
 这个Jira kafka在 2.0.0以上的版本中已经修复，这里可以忽略。
 
 综上，这个注释信息信息量太少了，还是直接看代码
 */
 
 
private[kafka010] class KafkaMicroBatchStream(
    private[kafka010] val kafkaOffsetReader: KafkaOffsetReader,
    executorKafkaParams: ju.Map[String, Object],
    options: CaseInsensitiveStringMap,
    metadataPath: String,
    startingOffsets: KafkaOffsetRangeLimit,
    failOnDataLoss: Boolean) extends SupportsAdmissionControl with MicroBatchStream with Logging {

//0.初始化参数，得到 Kafka Consumer poll的超时时间，默认120s
  private[kafka010] val pollTimeoutMs = options.getLong(
    KafkaSourceProvider.CONSUMER_POLL_TIMEOUT,
    SparkEnv.get.conf.get(NETWORK_TIMEOUT) * 1000L)

//1.当checkpoint不存在时，会走到这个方法，也就是这个Job第一次启动
// 初始化offset信息，从指定的offset开始读取数据，还是从earliest offset开始读取
// 看用户输入的参数。
// 1.当checkpoint 已经存在，说明Job重启了，应该从上一次的计算状态恢复
// Spark会自动从 checkpoint中读取未提交的offset,保障数据不丢,
//deserializeOffset方法 反序列化成KafkaSourceOffset对象，继续处理 
  override def initialOffset(): Offset = {
    val metadataLog =
      new KafkaSourceInitialOffsetWriter(SparkSession.getActiveSession.get, metadataPath)
   var partitionToOffset: Map[TopicPartition, Long] = metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case EarliestOffsetRangeLimit =>
          KafkaSourceOffset(kafkaOffsetReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit =>
          KafkaSourceOffset(kafkaOffsetReader.fetchLatestOffsets(None))
        case SpecificOffsetRangeLimit(p) =>
          kafkaOffsetReader.fetchSpecificOffsets(p, reportDataLoss)
        case SpecificTimestampRangeLimit(p) =>
          kafkaOffsetReader.fetchSpecificTimestampBasedOffsets(p, failsOnNoMatchingOffset = true)
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }.partitionToOffsets
    KafkaSourceOffset(partitionToOffset)
  }

// 用户是否要限制每一批数据的行数
  override def getDefaultReadLimit: ReadLimit = {
     val maxOffsetsPerTrigger = Option(options.get(
      KafkaSourceProvider.MAX_OFFSET_PER_TRIGGER)).map(_.toLong)
    maxOffsetsPerTrigger.map(ReadLimit.maxRows).getOrElse(super.getDefaultReadLimit)
  }


//2.kafkaOffsetReader.fetchLatestOffsets
//获取这个Topic下 所有partition的 latest offset
//具体步骤： kafkaconsumer.poll(0) --> kafkaconsumer.assignment获取所有分区
// 每个分区调一次kafkaconsumer.seekToEnd(partitions) 拿到最新offset，并校验。
 // 如果有readlimit，则进行裁剪。
  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = {
    val startPartitionOffsets = start.asInstanceOf[KafkaSourceOffset].partitionToOffsets
    val latestPartitionOffsets = kafkaOffsetReader.fetchLatestOffsets(Some(startPartitionOffsets))
    endPartitionOffsets = KafkaSourceOffset(readLimit match {
      case rows: ReadMaxRows =>
        rateLimit(rows.maxRows(), startPartitionOffsets, latestPartitionOffsets)
      case _: ReadAllAvailable =>
        latestPartitionOffsets
    })
    endPartitionOffsets
  }

//3.startPartitionOffsets，endPartitionOffsets都有了
//这里开始计算 每个partition上的 offsetRange
//比如 TopicA-partition1 offsetRange 1000~3000
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startPartitionOffsets = start.asInstanceOf[KafkaSourceOffset].partitionToOffsets
    val endPartitionOffsets = end.asInstanceOf[KafkaSourceOffset].partitionToOffsets

//计算过程中如果发现partition增加，会读取new Partition的earliest offset
//并根据“numPartitions”，“minPartitions” 参数计算生成多少个Spark tasks（Spark计算任务）
//默认一个Kafka Partition mapping 一个Spark task
    val offsetRanges = kafkaOffsetReader.getOffsetRangesFromResolvedOffsets(
      startPartitionOffsets,
      endPartitionOffsets,
      reportDataLoss
    )

    // 构造KafkaBatchInputPartition对象
    // 一个KafkaBatchInputPartition对象会mapping一个Spark task（Spark计算任务）
    offsetRanges.map { range =>
      KafkaBatchInputPartition(range, executorKafkaParams, pollTimeoutMs,
        failOnDataLoss, includeHeaders)
    }.toArray
  }

//4.每个KafkaBatchInputPartition对象会mapping一个Spark task（Spark计算任务）
// 这边就是构造 对每个partition读取数据用的对象
  override def createReaderFactory(): PartitionReaderFactory = {
    KafkaBatchReaderFactory
  }

// 1.工具方法，将json反序列化成 KafkaSourceOffset对象
// 比如Job重启时，Spark会自动从 checkpoint中读取未提交的offset
// 通过此方法 反序列化成对象，继续处理
// 保障数据不丢
  override def deserializeOffset(json: String): Offset = {
    KafkaSourceOffset(JsonUtils.partitionOffsets(json))
  }

//一批数据处理完成后，会走到这里，可以自定义操作，kafka这边啥也没干
  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {
    kafkaOffsetReader.close()
  }

}
```
checkPoint是在文件系统（磁盘/hdfs）中记录数据源最新的offset和已经提交的offset，已经计算的临时数据，metadata数据，当计算任务失败重启的时候，可以从checkPoint 恢复计算任务到上一次已经commit的状态。确保数据不会丢失也不会重复。


### 梳理一下执行逻辑。
#### 当Job第一次启动时，checkpoint还不存在
调用initialOffset()， 初始化offset信息，从指定的offset开始读取数据，还是从earliest offset开始读取，初始化后的offset 作为新一批处理的 startOffset
#### 当Job重启时，checkpoint已经存在
Spark会自动从从checkPoint读取到上一次已经commit的offset信息,deserializeOffset方法 
反序列化成KafkaSourceOffset对象，作为新一批处理的 startOffset
#### startOffset确定以后，下面的逻辑基本相同
latestOffset(start: Offset, readLimit: ReadLimit)，获取这个Topic下 所有partition的 latest offset。具体步骤： kafkaconsumer.poll(0) --> kafkaconsumer.assignment获取所有分区，每个分区调一次kafkaconsumer.seekToEnd(partitions) 拿到最新offset，并校验，如果有readlimit，则进行裁剪。
#### 映射成 Spark task（Spark 计算任务）
每个kafka partition 的 startPartitionOffsets，endPartitionOffsets都拿到了  
planInputPartitions(start: Offset, end: Offset):  每个partition上的 offsetRange  
比如 TopicA-partition1 offsetRange:1000~3000，每个topicPartition构造KafkaBatchInputPartition对象，再映射成一个Spark task（Spark 计算任务）  
**也就是：默认一个Kafka Partition mapping 一个Spark task**  
这个task中会包含kafka TopicPartition信息，要读取的offset范围，构造KafkaConsumer的必要参数等，后面会在Executor上构造KafkaConsumer并读取数据。

# KafkaBatchReaderFactory extends PartitionReaderFactory
```
object KafkaBatchReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
  
    val p = partition.asInstanceOf[KafkaBatchInputPartition]
    
    KafkaBatchPartitionReader(p.offsetRange, p.executorKafkaParams, p.pollTimeoutMs,
      p.failOnDataLoss, p.includeHeaders)
  }
}
```
这个半生对象比较简单，拿到一个KafkaBatchInputPartition，也就是封装了一个KafkaPartition和 这个Partition 上需要读取的Offset范围。  
每一个Executor进程会从Driver拿到KafkaBatchInputPartition，并通过这里的逻辑，new出KafkaBatchPartitionReader，去KafkaBroker上拉取数据，并转成InternalRow，进行下一步的计算。InternalRow可以理解为Spark中的数据集。

# KafkaBatchPartitionReader extends PartitionReader
```
private case class KafkaBatchPartitionReader(
    offsetRange: KafkaOffsetRange,
    executorKafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean,
    includeHeaders: Boolean) extends PartitionReader[InternalRow] with Logging {

// 1.获取KafkaDataConsumer，内部对KafkaConsumer进行了cache
  private val consumer = KafkaDataConsumer.acquire(offsetRange.topicPartition, executorKafkaParams)

//2.计算当前offsetRange是否在有startOffset< -1等奇怪的情况
  private val rangeToRead = resolveRange(offsetRange)
  
 //3.设置一个将Kafka ConsumerRecord[Array[Byte], Array[Byte]]
 //转成Spark InternalRow的转换器
  private val unsafeRowProjector = new KafkaRecordToRowConverter()
    .toUnsafeRowProjector(includeHeaders)

//4. 从第一个offset开始读取
  private var nextOffset = rangeToRead.fromOffset
 
 //5.迭代器模型
  private var nextRow: UnsafeRow = _

//6.迭代器模型
  override def next(): Boolean = {
  //6.1 判断offset是否在范围内
    if (nextOffset < rangeToRead.untilOffset) {
    //6.2 poll一条数据
      val record = consumer.get(nextOffset, rangeToRead.untilOffset, pollTimeoutMs, failOnDataLoss)
      if (record != null) {
        //6.3 将kafka ConsumerRecord转成 InternalRow，即将被迭代
        nextRow = unsafeRowProjector(record)
        //6.3 offset ++
        nextOffset = record.offset + 1
        true
      } else {
        false
      }
    } else {
      false
    }
  }

//7. 如果next() 方法返回true，执行器线程会调用这个方法拿InternalRow
  override def get(): UnsafeRow = {
    assert(nextRow != null)
    nextRow
  }

//8.一个微批的数据读取完成后，释放consumer对象
  override def close(): Unit = {
    consumer.release()
  }

}
```
这是 单个partition中指定范围 的Kafka数据的迭代器模型。

# KafkaDataConsumer
```
val record = consumer.get(nextOffset, rangeToRead.untilOffset, pollTimeoutMs, failOnDataLoss)
```
这个类已经不属于Spark 数据源API的一部分，在上文中，只在PartitionReader中被迭代时使用，这个类封装了对 KafkaConsumer的所有方法调用，包括assgin/seek/position/poll等方法。   

内部自定义了InternalKafkaConsumerPool（基于org.apache.commons.pool2.impl.GenericKeyedObjectPool实现）缓存 KafkaConsumer 

### consumer.get方法的实现
```

def get(
    offset: Long, // 开始位点
    untilOffset: Long, // 结束位点
    pollTimeoutMs: Long, // poll 超时时间
    failOnDataLoss: Boolean):// 当offset在Broker上已经过期，停止任务，避免数据丢失
  ConsumerRecord[Array[Byte], Array[Byte]] = runUninterruptiblyIfPossible {

//从InternalKafkaConsumerPool获取一个InternalKafkaConsumer，如果pool中没有则创建
//创建过程中会调用KafkaConsumer.assgin(TopicPartion)
//基本类似Map.computeIfAbsent
//缓存的key 为本次任务的id+ TopicPartition
  val consumer: InternalKafkaConsumer = getOrRetrieveConsumer()
  
  //与上面类似，获取kafka数据缓存区，因为每次poll到的数据量可能大于
  //需要的数据量，所以poll到的数据会先缓存起来
  val fetchedData: FetchedData  = getOrRetrieveFetchedData(offset)

  var isFetchComplete = false

  while (toFetchOffset != UNKNOWN_OFFSET && !isFetchComplete) {
    try {
    //fetchRecord方法见下文
      fetchedRecord = fetchRecord(consumer, fetchedData, toFetchOffset, untilOffset,
        pollTimeoutMs, failOnDataLoss)
        
        //获取到数据，结束
      if (fetchedRecord.record != null) {
        isFetchComplete = true
      } else {
      
      //如果指定的 fromOffset 在Kafka Broker上已经过期
      //找找缓存中有没有 `[fromOffset, untilOffset)` 这个范围内的数据。
      //找到的话会 掉过一部分数据。
        toFetchOffset = fetchedRecord.nextOffsetToFetch
        if (toFetchOffset >= untilOffset) {
          fetchedData.reset()
          toFetchOffset = UNKNOWN_OFFSET
        } else {
          logDebug(s"Skipped offsets [$offset, $toFetchOffset]")
        }
      }
      
    } catch {
    
      case e: OffsetOutOfRangeException =>
      //遇到异常，清理缓存的KafkaConsumer 和 数据缓存
        releaseConsumer()
        fetchedData.reset()
        
        //根据failOnDataLoss参数的设置
        //true:抛出异常到外层，结束Job，防止数据丢失 （推荐）
        //false:跳过这条数据，继续Job，部分数据丢失
        reportDataLoss(topicPartition, groupId, failOnDataLoss,
          s"Cannot fetch offset $toFetchOffset", e)
        toFetchOffset = getEarliestAvailableOffsetBetween(consumer, toFetchOffset, untilOffset)
    }
  }

  if (isFetchComplete) {
    fetchedRecord.record
  } else {
    fetchedData.reset()
    null
  }
}
```
小总结  
1.KafkaConsumer.assgin(TopicPartion)后包装成InternalKafkaConsumer放进InternalKafkaConsumerPool  
2. failOnDataLoss = true时，处理简单粗暴，当OffsetOutOfRangeException时直接抛出异常，终止Job（也就是计算任务StreamQuery）  
3.failOnDataLoss = false时，打印日志，跳过offset，继续读。

### fetchRecord方法的实现
这边fetchRecord，fetchRecords，fetchData，fetchedData等等，有点绕，我对代码进行简化，去掉了一些offset异常而throw Exception的情况。
```
/**
 * Get the fetched record for the given offset if available.
 *
 * @throws OffsetOutOfRangeException if `offset` is out of range
 * @throws TimeoutException if cannot fetch the record in `pollTimeoutMs` milliseconds.
 */
private def fetchRecord(
    consumer: InternalKafkaConsumer,
    fetchedData: FetchedData,
    offset: Long,
    untilOffset: Long,
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean): FetchedRecord = {
    //如果fromOffset在缓存中不存在
  if (offset != fetchedData.nextOffsetInFetchedData) {
  //进行KafkaConsumer.seek && KafkaConsumer.poll 见下文
    fetch(consumer, fetchedData, offset, pollTimeoutMs)
  }
  //poll到数据后包装并返回
    val record = fetchedData.next()
   fetchedRecord.withRecord(record, fetchedData.nextOffsetInFetchedData)
}
```
### fetch方法的实现


```
def fetch(offset: Long, pollTimeoutMs: Long):
    (ju.List[ConsumerRecord[Array[Byte], Array[Byte]]], Long, AvailableOffsetRange) = {

//1.先 seek到指定位置
  consumer.seek(topicPartition, offset)
  //2.拉取数据
  val p: ConsumerRecords[Array[Byte], Array[Byte]] = consumer.poll(Duration.ofMillis(pollTimeoutMs))
  // 3.过滤出指定partition的数据
  val r = p.records(topicPartition)
//4. 获取当前consumer的最新offset
val offsetAfterPoll = consumer.position(topicPartition)
  logDebug(s"Offset changed from $offset to $offsetAfterPoll after polling")
  //5. 获取partition earliest和latest Offset，判断有没有超出。
  val range = getAvailableOffsetRange()
  val fetchedData = (r, offsetAfterPoll, range)
  
  fetchedData
}
```
到这，数据源数据拉取的全流程就解析完了。

# 总结前面的问题
- Kafka Topic中的消息来一条就会写入InputTable吗？
```
并不是，只有一个微批处理的开始，数据源会去检测Topic下每个partition offsets是否有更新。
如果Trigger间隔时间很长，那么这段时间内Topic中新增的数据，Spark是无感知的。
```

- Structured Streaming + Kafka 如何实现数据不丢？
```
对于Source来说：
当一个微批处理开始的时候，Driver进程将offset写入checkpoint中，
当一个微批处理完成时（Sink成功），Driver进程将commit信息写入checkpoint中，
当Job失败重启的时候，可以从checkpoint中恢复Job到上一个commit的状态，保障数据不重复不丢失。

```
- Structured Streaming 没有用Kafka的消费者重平衡机制，如何分配消费者和partition的关系？
```
Spark每一个微批处理开始前都是先获取Topic下所有Partition的最新Offset（LEO），和上次已经处理完成的Offset对比
计算有哪些partition上有offset更新，以及要拉取的offset范围。
在Driver进程中，这些partition一对一映射成为Spark Task（Spark计算任务），
组成TaskPool，由Executor执行Task，至于Task的调度由Spark内部实现。
所以，默认参数情况下，如果Topic下Partition数量远大于Executor数量，会导致数据拉取执行缓慢
可以增加Executor或者通过numPartitions参数调整
```
- Structured Streaming 如何实现 Kafka Offsets 管理？
```
Spark每一个微批处理开始前都是先获取Topic下所有Partition的最新Offset（LEO）
根据readlimit参数，计算每个partition的 untilOffset写入checkpoint

处理过程中，用Kafka Consumer的seek方法，指定offset消费，只消费[fromOffset,untilOffset) 范围内的数据，左闭右开。

当一批数据处理完成后写入commit信息
```
- 自研MQ如何实现 最新的数据源接口，成为Spark的Source？
```
Data Source interfaces in Spark 3.0.0
主要有以下的接口
-   TableProvider
-   Table
-   ScanBuilder
-   Scan
-   MicroBatchStream
-   InputPartition
-   PartitionReaderFactory
-   PartitionReader
实现以上接口即可。

官方sample Java版
https://github.com/apache/spark/blob/72615bc551adaa238d15a8b43a8f99aaf741c30f/sql/core/src/test/java/test/org/apache/spark/sql/connector/JavaPartitionAwareDataSource.java
```
- Spark 输入输出源相关接口API V1 V2 多次迭代的原因是什么？
```
详细视频：-   https://www.youtube.com/watch?v=9-eomYXVnvY&ab_channel=Databricks
主要的改进是 并发读取数据、过滤器下推(filters push down)、加速聚合操作，增加Spark Data Encoding、数据统计和分片、支持Structured Streaming等
```
