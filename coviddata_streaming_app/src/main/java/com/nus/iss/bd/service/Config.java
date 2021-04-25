package com.nus.iss.bd.service;

public class Config {
    public final static String hdfsHost = "hdfs://localhost:9000";
    public final static String kafkaHost = "localhost:9092";
    public final static String hdfsRootPath = "/SafeEntry_Analytics/";
    public final static String hdfsCaseFilePath = hdfsHost + hdfsRootPath + "case_streaming.parquet";
    public final static String hdfsGraphEdgeFilePath = hdfsHost + hdfsRootPath + "contact_graph_edge.parquet";
    public final static String hdfsGraphVertexFilePath = hdfsHost + hdfsRootPath + "contact_graph_vertex.parquet";
    public final static String kafkaTopic = "covid_case";
}
