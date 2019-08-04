package com.dataart.bigdata.bu;

import java.io.Serializable;

public class BrowserStats implements Serializable {
    private String agentName;
    private String visitDate;
    private long numIp;

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

    public long getNumIp() {
        return numIp;
    }

    public void setNumIp(long numIp) {
        this.numIp = numIp;
    }

    @Override
    public String toString() {
        return "BrowserStats{" +
                "agentName='" + agentName + '\'' +
                ", visitDate='" + visitDate + '\'' +
                ", numIp=" + numIp +
                '}';
    }
}
