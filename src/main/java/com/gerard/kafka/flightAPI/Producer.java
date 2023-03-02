package com.gerard.kafka.flightAPI;

import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    public static void getFlightData(String user, String password) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(Producer.class);

        String bootstrapServers = "127.0.0.1:9092";
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("Closing producer...");
            producer.close();
            logger.info("Done!");
        }));

        //Receive JSON String
        String json = null;
        try {
            json = OpenSkyClient.getFlightData(user, password);
        } catch (IOException e) {
            logger.error("Could not load properties file", e);
        }
        // Transform JSON String to JSON Object
        JSONObject jsonObject = new JSONObject(json);
        JSONArray flightStates = jsonObject.getJSONArray("states");
        // GET JSON LENGTH
        int keyCount = flightStates.length();

        for (int i = 0; i < keyCount; i++){

            // create a producer record
            JSONObject state = flightStates.getJSONObject(i);
            String topic = "first_topic";
            String value = state.toString();
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key);

            //send data - async
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null){
                        logger.error("Something bad happened", e);
                    }
                }
            });

        }
    }
}