package com.netflix.titus.es.publish;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EsTasksPublish {
    private static final Logger logger = LoggerFactory.getLogger(EsTasksPublish.class);

    public static void main(String[] args) {
        SpringApplication.run(EsTasksPublish.class, args);
    }
}
