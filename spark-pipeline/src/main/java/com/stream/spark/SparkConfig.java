package com.stream.spark;

import org.apache.spark.SparkConf;


public class SparkConfig {

    
    public SparkConf sparkConf() {
        return new SparkConf()
                .setAppName("JavaDirectKafka")
                .setMaster("local[*]")
                .set("spark.executor.memory","1g");
    }
}
