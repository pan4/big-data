package com.dataart.bigdata.bu;

import java.io.Serializable;

public class BrowserStats implements Serializable {
    private String agentName;
    private String visitDate;
    private String city;

    public String getAgentName() {
        return agentName;
    }

    public void setAgentName(String agentName) {
        this.agentName = agentName;
    }

    public String getVisitDate() {
        return visitDate;
    }

    public void setVisitDate(String visitDate) {
        this.visitDate = visitDate;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "BrowserStats{" +
                "agentName='" + agentName + '\'' +
                ", visitDate='" + visitDate + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
