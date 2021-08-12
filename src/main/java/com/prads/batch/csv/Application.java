package com.prads.batch.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class Application {
    private final static Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        LOGGER.info(":: BATCH CSV - Grouping lines - is starting.");
        SpringApplication.run(Application.class, args);
    }
}