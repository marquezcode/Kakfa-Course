package com.gerard.kafka.flightAPI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

public class Main {
    public static String USERNAME;
    public static String PASSWORD;
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Main.class.getName());
        //LOAD CREDENTIALS FOR OPENSKYAPI FROM PROPERTIES FILE
        Properties prop = new Properties();
        String propertiesPath = "/home/gerard/IdeaProjects/kafka.properties";
        InputStream stream;

        //input stream creation
        try {
            stream = new FileInputStream(new File(propertiesPath));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        //load properties stream
        try {
            //load a properties file from class path, inside static method
            prop.load(stream);

            //get the property value and print it out
            USERNAME = prop.getProperty("user");
            PASSWORD = prop.getProperty("pwd");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            stream.close();
        } catch (IOException e) {
            logger.error("Couldn't close stream input for properties", e);
        }

        //setup timer for producers
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    Producer.getFlightData(USERNAME, PASSWORD);
                } catch (ExecutionException e) {
                    logger.error("Could not recover states", e);
                } catch (InterruptedException e) {
                    logger.error("Could not recover states", e);;
                }
            }
        };
        timer.scheduleAtFixedRate(task, 100, 1000);
    }
}