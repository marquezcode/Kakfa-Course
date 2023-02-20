package com.gerard.kafka.flightAPI;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensky.api.OpenSkyApi;
import org.opensky.model.OpenSkyStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


public class OpenSkyClient {



    public static String getFlightData(String user, String password) throws IOException {


        Logger logger = LoggerFactory.getLogger(Main.class.getName());

        OpenSkyApi api = new OpenSkyApi(user, password);

        ObjectMapper mapper = new ObjectMapper();
        String jsonString = null;

        try {
            OpenSkyStates os = api.getStates(0, null);
            jsonString = mapper.writeValueAsString(os);
        } catch (IOException e) {
            logger.error("Could not recover states", e);
        }
        return jsonString;
    }
}