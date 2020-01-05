package com.redhat.emergency.response.incident.priority.rules.model;

public class AveragePriority {

    private int count;

    private double total;

    public AveragePriority() {
        total = 0.0;
    }

    public void accumulate(Number value) {
        if (value != null) {
            this.total += value.doubleValue();
        }
    }

    public void reverse(Number value) {
        if (value != null) {
            this.count--;
            this.total -= value.doubleValue();
        }
        if (count == 0) {
            total = 0.0;
        }
    }

    public void add() {
        this.count++;
    }

    public void retract() {
        this.count--;
    }

    public Double getResult() {
        if (count == 0) {
            return 0.0;
        } else {
            return total / count;
        }
    }
}
