package com.redhat.emergency.response.incident.priority;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import com.redhat.emergency.response.incident.priority.rules.model.AveragePriority;
import com.redhat.emergency.response.incident.priority.rules.model.IncidentAssignmentEvent;
import com.redhat.emergency.response.incident.priority.rules.model.IncidentPriority;
import com.redhat.emergency.response.incident.priority.rules.model.PriorityZone;
import com.redhat.emergency.response.incident.priority.rules.model.PriorityZoneApplicationEvent;

import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.Message;
import org.drools.core.io.impl.ClassPathResource;
import org.kie.api.KieBase;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.io.Resource;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.QueryResults;
import org.kie.api.runtime.rule.QueryResultsRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RulesVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(RulesVerticle.class);

    private KieBase kbase;

    private KieSession ksession;

    @Override
    public Completable rxStart() {
        
        return Completable.fromMaybe(vertx.<Void>rxExecuteBlocking(future -> {
            try {
                kbase = setupKieBase("com/redhat/emergency/response/incident/priority/rules/priority_rules.drl");
                initSession();

                vertx.eventBus().consumer("incident-assignment-event", this::assignmentEvent);
                vertx.eventBus().consumer("incident-priority", this::incidentPriority);
                vertx.eventBus().consumer("priority-zone-application-event", this::priorityZone);
                vertx.eventBus().consumer("reset", this::reset);

                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }));
    }

    private void initSession() {
        if (ksession != null) {
            ksession.dispose();
        }
        ksession = kbase.newKieSession();
        Logger logger = LoggerFactory.getLogger("PriorityRules");
        ksession.setGlobal("logger", logger);
    }

    private void reset(Message<JsonObject> message) {
        log.info("Resetting ksession");
        initSession();
        message.reply(new JsonObject());
    }

    private void assignmentEvent(Message<JsonObject> message) {
        log.debug("AssignmentEvent - received message: " + message.body().toString());

        IncidentAssignmentEvent incidentAssignmentEvent = new IncidentAssignmentEvent(message.body().getString("incidentId"),
                message.body().getBoolean("assignment"));
        ksession.insert(incidentAssignmentEvent);
        ksession.fireAllRules();

        if (! message.body().getBoolean("assignment")) {
            checkPriorityZones(message.body().getString("incidentId"));
        }
    }

    private void incidentPriority(Message<JsonObject> message) {
        log.debug("IncidentPriority - received message: " + message.body().toString());
        String incidentId = message.body().getString("incidentId");
        QueryResults results = ksession.getQueryResults("incidentPriority", incidentId);
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        Integer priority;
        if (row == null) {
            priority =0;
        } else {
            priority = ((IncidentPriority)row.get("incidentPriority")).getPriority();
        }

        results = ksession.getQueryResults("averagePriority");
        row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        Double average;
        if (row == null) {
            average = 0.0;
        } else {
            average = ((AveragePriority)row.get("averagePriority")).getAveragePriority();
        }

        results = ksession.getQueryResults("incidents");
        JsonObject jsonObject = new JsonObject().put("incidentId", incidentId).put("priority", priority)
                .put("average", average).put("incidents", results.size());
        message.reply(jsonObject);
    }

    /**
     * Add or update a priority zone
     * @param message the PriorityZoneApplicationEvent from kafka
     */
    private void priorityZone(Message<JsonObject> message) {
        log.debug("PriorityZoneApplicationEvent - received message {}", message.body());
        String id = message.body().getString("id");
        double lat = message.body().getDouble("lat");
        double lon = message.body().getDouble("lon");
        double radius = message.body().getDouble("radius");

        PriorityZone priorityZone = new PriorityZone(id, lat, lon, radius);
        PriorityZoneApplicationEvent event = new PriorityZoneApplicationEvent(priorityZone);
        ksession.insert(event);

        QueryResults results = ksession.getQueryResults("incidents");
        results.forEach(row -> {
            IncidentPriority priority = ((IncidentPriority)row.get("incidentPriority"));
            if (inPriorityZone(priority.getIncident(), priorityZone)) {
                priority.setNeedsEscalation(true);
                ksession.update(row.getFactHandle("incidentPriority"), priority);
            }
        });

        ksession.fireAllRules();
    }


    private KieBase setupKieBase(String... resources) {
        KieServices ks = KieServices.Factory.get();
        KieBaseConfiguration config = ks.newKieBaseConfiguration();
        config.setOption( EventProcessingOption.STREAM );
        KieFileSystem kfs = ks.newKieFileSystem();
        KieRepository kr = ks.getRepository();

        for (String res : resources) {
            Resource resource = new ClassPathResource(res);
            kfs.write("src/main/resources/" + res, resource);
        }

        KieBuilder kb = ks.newKieBuilder(kfs);
        kb.buildAll();
        hasErrors(kb);

        KieContainer kc = ks.newKieContainer(kr.getDefaultReleaseId());

        return kc.newKieBase(config);
    }

    private void hasErrors(KieBuilder kbuilder) {
        if (kbuilder.getResults().hasMessages(org.kie.api.builder.Message.Level.ERROR)) {
            throw new RuntimeException("Build errors\n" + kbuilder.getResults().toString());
        }
    }

    @Override
    public Completable rxStop() {
        if (ksession != null) {
            ksession.dispose();
        }
        return Completable.complete();
    }

    /**
     * Check all known priority zones to determine whether this incident falls within them. If
     * it falls within one or more zones AND it hasn't yet been escalated, we need to update the escalation flag
     * and re-run rules.
     * @param incidentId the id of the incident to check
     */
    public void checkPriorityZones(String incidentId) {
        AtomicInteger withinZones = new AtomicInteger(0);
        QueryResults priorityZones = ksession.getQueryResults("priorityZones");
        priorityZones.forEach(row -> {
            PriorityZone priorityZone = (PriorityZone) row.get("priorityZone");
            if (inPriorityZone(incidentId, priorityZone)) {
                withinZones.getAndIncrement();
            }
        });

        if (withinZones.get() > 0) {
            QueryResults results = ksession.getQueryResults("incidentPriority", incidentId);
            QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
            if ( row != null ) {
                IncidentPriority incidentPriority = (IncidentPriority) row.get("incidentPriority");
                if (!incidentPriority.getEscalated()) {
                    incidentPriority.setNeedsEscalation(true);
                    ksession.update(row.getFactHandle("incidentPriority"), incidentPriority);
                    ksession.fireAllRules();
                }
            }
        }
    }

    /**
     * Checks whether the specified incident falls within the given priority zone.
     * 
     * @param incidentId the incident id
     * @param priorityZone the priority zone
     * @return true if the incident falls within the zone
     */
    public boolean inPriorityZone(String incidentId, PriorityZone priorityZone) {
        WebClient client = WebClient.create(this.getVertx());
        AtomicBoolean inPriorityZone = new AtomicBoolean(false);
        client.get("http://user4-incident-service.apps.cluster-e222.e222.example.opentlc.com/incidents/incident/3a64e1f5-848c-42af-bc8c-abce650d4e46")
            .send(ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result().bodyAsJsonObject();
              
                    double incidentLat = Double.parseDouble(response.getString("lat"));
                    double incidentLon = Double.parseDouble(response.getString("lon"));

                    inPriorityZone.set(distance(incidentLat, incidentLon, priorityZone.getLat(), priorityZone.getLon(), "K") <= priorityZone.getRadius());
                  } else {
                    System.out.println("Something went wrong " + ar.cause().getMessage());
                  }
            });

        client.close();
        return inPriorityZone.get();
    }

    /**
     * Calculate the distance between two coordinates in latitude and longitude, using the specified units.
     * 
     * @param lat1 the latitude of the first point
     * @param lon1 the longitude of the first point
     * @param lat2 the latitude of the second point
     * @param lon2 the longitude of the second point
     * @param unit the unit of measurement, where K = kilometers, M = miles (defualt), and N = nautical miles
     * @return the distance as a double
     */
    private static double distance(double lat1, double lon1, double lat2, double lon2, String unit) {
		if ((lat1 == lat2) && (lon1 == lon2)) {
			return 0;
		}
		else {
			double theta = lon1 - lon2;
			double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
			dist = Math.acos(dist);
			dist = Math.toDegrees(dist);
			dist = dist * 60 * 1.1515;
			if (unit.equals("K")) {
				dist = dist * 1.609344;
			} else if (unit.equals("N")) {
				dist = dist * 0.8684;
			}
			return (dist);
		}
	}
}
