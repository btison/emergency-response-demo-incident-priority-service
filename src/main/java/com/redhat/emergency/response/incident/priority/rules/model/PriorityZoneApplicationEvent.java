package com.redhat.emergency.response.incident.priority.rules.model;

public class PriorityZoneApplicationEvent {

    private PriorityZone priorityZone;

    public PriorityZoneApplicationEvent(PriorityZone priorityZone) {
        this.priorityZone = priorityZone;
    }

    public PriorityZone getPriorityZone() {
        return this.priorityZone;
    }

    public void setPriorityZoneId(PriorityZone priorityZone) {
        this.priorityZone = priorityZone;
    }
}
