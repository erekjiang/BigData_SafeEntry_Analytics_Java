package com.nus.iss.bd;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
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
import scala.Tuple2;

import java.util.*;

public class TestKafkaStream {

    public static JavaSparkContext sparkContext;


    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("covid_case");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("WordCountingAppWithCheckpoint");
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerialize");
        sparkConf.registerKryoClasses((Class<ConsumerRecord>[] )Arrays.asList(ConsumerRecord.class).toArray());

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        sparkContext = streamingContext.sparkContext();

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

//        JavaPairDStream<String, String> results = messages.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        messages.foreachRDD( rdd ->{
            List<ConsumerRecord<String, String>> results =  rdd.collect();
            results.forEach(result ->{
                System.out.println(result);
                System.out.printf("Received key : %s, key: %s %n",result.key(),result.value());
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();


    }
}
