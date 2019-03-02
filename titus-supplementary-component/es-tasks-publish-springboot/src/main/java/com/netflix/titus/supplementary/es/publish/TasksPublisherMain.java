package com.netflix.titus.supplementary.es.publish;


import com.netflix.titus.supplementary.es.publish.config.TasksPublisherConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({TasksPublisherConfiguration.class})
public class TasksPublisherMain {
    private static final Logger logger = LoggerFactory.getLogger(TasksPublisherMain.class);

    public static void main(String[] args) {
        SpringApplication.run(TasksPublisherMain.class, args);
    }
}
