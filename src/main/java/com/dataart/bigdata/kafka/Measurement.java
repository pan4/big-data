package com.dataart.bigdata.kafka;

public class Measurement {
    String device;
    Long timestamp;
    Integer value;

    public Measurement() {
    }

    public Measurement(String device, Long timestamp, Integer value) {
        this.device = device;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Measurement{" +
                "device='" + device + '\'' +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
