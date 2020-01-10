package com.redhat.emergency.response.incident.priority.rules.model;

import io.vertx.ext.web.client.WebClient;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class IncidentPriority {

    private String incident;

    private Integer priority;

    private boolean needsEscalation;

    private boolean escalated;

    private double lat;

    private double lon;

    public IncidentPriority(String incident) {
        this.incident = incident;
        this.priority = 0;
        this.escalated = false;
        this.needsEscalation = false;

        // retrieveCoordinates();
    }

    public String getIncident() {
        return incident;
    }

    public void setIncident(String incident) {
        this.incident = incident;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }


    public boolean getNeedsEscalation() {
        return needsEscalation;
    }

    public void setNeedsEscalation(boolean needsEscalation) {
        this.needsEscalation = needsEscalation;
    }

    public boolean getEscalated() {
        return escalated;
    }

    public void setEscalated(boolean escalated) {
        this.escalated = escalated;
    }

    public double getLat() {
        return this.lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return this.lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    private void retrieveCoordinates() {
        String incidentUrl = Vertx.currentContext().config().getString("incident-service");
        if (incidentUrl == null) {
            incidentUrl = "http://user4-incident-service.apps.cluster-e222.e222.example.opentlc.com/";
        }

        WebClient client = WebClient.create(Vertx.currentContext().owner());
        client.get(incidentUrl + "/incidents/incident/" + incident)
            .send(ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result().bodyAsJsonObject();
              
                    this.lat = Double.parseDouble(response.getString("lat"));
                    this.lon = Double.parseDouble(response.getString("lon"));
                  } else {
                    System.out.println("Something went wrong " + ar.cause().getMessage());
                  }
            });

        client.close();
    }
}
