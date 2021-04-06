package com.nus.iss.bd;

import com.nus.iss.bd.dto.CaseRecordDto;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import scala.Tuple2;

import java.util.*;

public class KafkaSparkStream {

    public static JavaSparkContext sparkContext;
    public static SparkSession spark;
    public static JavaStreamingContext streamingContext;

    public final static String hdfsHost = "hdfs://localhost:9000";
    public final static String hdfsRootPath = "/SafeEntry_Analytics/";
    public final static String hdfsCaseFilePath = hdfsHost + hdfsRootPath + "case_streaming.parquet";

    private static Logger logger = Logger.getLogger(KafkaSparkStream.class);

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        initSparkAndStreamingContext();

        Collection<String> topics = Arrays.asList("covid_case");
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams()));

        messages.foreachRDD(rdd -> {
            List<ConsumerRecord<String, String>> results = rdd.collect();

            results.forEach(result -> {
                logger.info(result);
                //read json as dataframe and store to hdfs
                JavaRDD<String> jsonRDD = sparkContext.parallelize(Arrays.asList(result.value()));
                Dataset<Row> df = spark.read().json(jsonRDD);
                df.write().mode(SaveMode.Append).parquet(hdfsCaseFilePath);
                logger.info(String.format("Received key : %s, key: %s %n", result.key(), result.value()));
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();


    }

    private static Map<String, Object> kafkaParams(){
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }

    public static void initSparkAndStreamingContext(){

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Kafka covid_case Streaming");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerialize");
        sparkConf.registerKryoClasses((Class<ConsumerRecord>[]) Arrays.asList(ConsumerRecord.class).toArray());


        streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        sparkContext = streamingContext.sparkContext();
        spark = SparkSession.builder().config(sparkConf).getOrCreate();

    }

    public void Store() {

    }
}
