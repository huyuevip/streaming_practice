package job;

import data.KafkaPayloadSchema;
import data.Payload;
import data.PayloadPlus;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.configuration.CheckpointingOptions;

/*
Author: Yue Hu
Date: 2024/11/5
*/

public class StreamingJob {
    private SourceFunction<Long> source;
    private SinkFunction<Long> oddSink;
    private SinkFunction<Long> evenSink;
    private SinkFunction<Long> fileSink;

    public StreamingJob(SourceFunction<Long> source, SinkFunction<Long> oddSink,SinkFunction<Long> evenSink,SinkFunction<Long> fileSink) {
        this.source = source;
        this.oddSink = oddSink;
        this.evenSink = evenSink;
        this.fileSink = fileSink;
    }

    public StreamingJob() {
    }

    public void execute() throws Exception {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        //conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs:///flink/checkpoint/dir");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "D:\\flink\\temp");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(1000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        KafkaSource kafkaSource = KafkaSource.<Payload>builder()
                .setBootstrapServers("localhost:9094")
                .setTopics("test_topic_1")
                .setGroupId("flinkgroup")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new KafkaPayloadSchema())
                .build();


        DataStream<Payload> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(1);

        stream.print();

        Table payloadTable =tableEnv.fromDataStream(stream);
        payloadTable.printSchema();

        tableEnv.createTemporaryView("payloadTableView", payloadTable);

        Table resultOddTable = tableEnv.sqlQuery("select name,address,dateOfBirth,TIMESTAMPDIFF(YEAR,CAST(dateOfBirth as TIMESTAMP),CAST(CURRENT_DATE() as TIMESTAMP)) as age from payloadTableView where TIMESTAMPDIFF(YEAR,CAST(dateOfBirth as TIMESTAMP),CAST(CURRENT_DATE() as TIMESTAMP))%2=1");
        DataStream resultOddStream = tableEnv.toChangelogStream(resultOddTable);

        resultOddStream.print();

        Table resultEvenTable = tableEnv.sqlQuery("select name,address,dateOfBirth,TIMESTAMPDIFF(YEAR,CAST(dateOfBirth as TIMESTAMP),CAST(CURRENT_DATE() as TIMESTAMP)) as age from payloadTableView where TIMESTAMPDIFF(YEAR,CAST(dateOfBirth as TIMESTAMP),CAST(CURRENT_DATE() as TIMESTAMP))%2=0");
        DataStream resultEvenStream = tableEnv.toChangelogStream(resultEvenTable);

        resultEvenStream.print();

        JsonSerializationSchema<PayloadPlus> jsonFormat=new JsonSerializationSchema<>();
        KafkaSink<PayloadPlus> oddSink = KafkaSink.<PayloadPlus>builder()
                .setBootstrapServers("localhost:9094")
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<>().setValueSerializationSchema(jsonFormat).setTopic("ODD_TOPIC").build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        resultOddStream.sinkTo(oddSink);


        KafkaSink<PayloadPlus> evenSink = KafkaSink.<PayloadPlus>builder()
                .setBootstrapServers("localhost:9094")
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<>().setValueSerializationSchema(jsonFormat).setTopic("EVEN_TOPIC").build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        resultEvenStream.sinkTo(evenSink);


        FileSink<String> fileSink = FileSink
                .forRowFormat(new Path("d:\\flink_kafka_data"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy( DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(30))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .build())
                .build();

        stream.map(Payload::toString).sinkTo(fileSink);

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        StreamingJob job = new StreamingJob();
        job.execute();

    }

}
