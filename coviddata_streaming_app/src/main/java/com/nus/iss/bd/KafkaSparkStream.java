package com.nus.iss.bd;

import com.nus.iss.bd.service.SparkStreamingService;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KafkaSparkStream {

    public static void main(String[] args) throws InterruptedException
    {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkStreamingService sparkStreamingService = new SparkStreamingService();
        sparkStreamingService.run();
    }


}
