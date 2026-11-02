package com.demo.streaming.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ServingApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(ServingApiApplication.class, args);
    }
}

