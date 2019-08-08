package com.dataart.bigdata.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class MeasurementDeserializer implements Deserializer<Measurement> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Measurement deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        Measurement measurement = null;
        try {
            measurement = mapper.readValue(bytes, Measurement.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return measurement;
    }

    @Override
    public void close() {

    }
}
