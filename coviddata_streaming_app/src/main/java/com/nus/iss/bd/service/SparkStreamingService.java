package com.nus.iss.bd.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nus.iss.bd.KafkaSparkStream;
import com.nus.iss.bd.dto.CaseRecordDto;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.graphframes.GraphFrame;
import org.graphframes.lib.ShortestPaths;

import java.util.*;

import static com.nus.iss.bd.service.Config.*;

public class SparkStreamingService {

    private static Logger logger = Logger.getLogger(KafkaSparkStream.class);

    public JavaSparkContext sparkContext;
    public SparkSession spark;
    public JavaStreamingContext streamingContext;


    public static Dataset<Row> graphEdgeDF;
    public static Dataset<Row> graphVertexDF;
    public static GraphFrame graphFrame;
    Map<String, Object> kafkaParams;

    public SparkStreamingService() {
        initSparkAndStreamingContext();
        initKafkaParams();
    }

    private void initSparkAndStreamingContext(){

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Kafka covid_case Streaming");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerialize");
        sparkConf.registerKryoClasses((Class<ConsumerRecord>[]) Arrays.asList(ConsumerRecord.class).toArray());

        streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        sparkContext = streamingContext.sparkContext();
        spark = SparkSession.builder().config(sparkConf).getOrCreate();

    }

    private void initKafkaParams(){
        kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
    }

    public void run() throws InterruptedException {

        readGraphData();

        Collection<String> topics = Arrays.asList(kafkaTopic);
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        messages.foreachRDD(rdd -> {
            List<ConsumerRecord<String, String>> results = rdd.collect();

            results.forEach(result -> {
                logger.info(result);
                //read json as dataframe and store to hdfs
                CaseRecordDto caseRecordDto = null;
                try {
                    caseRecordDto = objectMapper.readValue(result.value(),CaseRecordDto.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

                processAnalytics(new ArrayList<>(Arrays.asList(caseRecordDto.getNric())));

//                JavaRDD<String> jsonRDD = sparkContext.parallelize(Arrays.asList(result.value()));
//                Dataset<Row> df = spark.read().json(jsonRDD);
//                df.write().mode(SaveMode.Append).parquet(hdfsCaseFilePath);
                logger.info(String.format("Received key : %s, key: %s %n", result.key(), result.value()));
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void readGraphData(){
        graphEdgeDF = spark.read().parquet(hdfsGraphEdgeFilePath);
        graphVertexDF = spark.read().parquet(hdfsGraphVertexFilePath);
        graphFrame = new GraphFrame(graphVertexDF,graphEdgeDF);
    }

    public void processAnalytics(ArrayList<Object> cases){
        StopWatch watch = new StopWatch();

        watch.start();
        ShortestPaths shortestPaths = graphFrame.shortestPaths().landmarks(cases);
        watch.stop();
        logger.info("Time Elapsed for shortestPaths(): " + watch.getTime()); // Prints: Time Elapsed: 2501

        watch.reset();
        watch.start();
        Dataset<Row> results = shortestPaths.run()
                .select(new Column("id"), functions.explode(new Column("distances")))
                .filter("value = 1");
        results.show();
        watch.stop();
        logger.info("Time Elapsed for filtering result : " + watch.getTime()); // Prints: Time Elapsed: 2501

    }
}
