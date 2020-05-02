package com.redhat.emergency.response.incident.priority;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import com.redhat.emergency.response.incident.priority.rules.model.AveragePriority;
import com.redhat.emergency.response.incident.priority.rules.model.IncidentAssignmentEvent;
import com.redhat.emergency.response.incident.priority.rules.model.IncidentPriority;
import com.redhat.emergency.response.incident.priority.rules.model.PriorityZone;
import com.redhat.emergency.response.incident.priority.rules.model.PriorityZoneApplicationEvent;
import com.redhat.emergency.response.incident.priority.rules.model.PriorityZoneClearEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.reactivex.Completable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.backends.BackendRegistries;
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

    private final Integer priorityZoneUpsurge = 50;

    private Timer assignmentTimer;

    private Timer queryTimer;

    private AtomicInteger kieSessionDepth;

    @Override
    public Completable rxStart() {
        
        return Completable.fromMaybe(vertx.<Void>rxExecuteBlocking(future -> {
            try {
                kbase = setupKieBase("com/redhat/emergency/response/incident/priority/rules/priority_rules.drl");
                initSession();

                vertx.eventBus().consumer("incident-assignment-event", this::assignmentEvent);
                vertx.eventBus().consumer("incident-priority", this::incidentPriority);
                vertx.eventBus().consumer("priority-zones", this::priorityZones);
                vertx.eventBus().consumer("priority-zone-application-event", this::priorityZone);
                vertx.eventBus().consumer("priority-zone-clear-event", this::clearPriorityZones);
                vertx.eventBus().consumer("reset", this::reset);

                MeterRegistry registry = BackendRegistries.getDefaultNow();
                assignmentTimer = Timer.builder("rules.assignment.timer").register(registry);
                queryTimer = Timer.builder("rules.query.timer").register(registry);
                kieSessionDepth = registry.gauge("kiesession.incidents", new AtomicInteger(0));

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
        ksession.setGlobal("priorityZoneUpsurge", priorityZoneUpsurge);
    }

    private void reset(Message<JsonObject> message) {
        log.info("Resetting ksession");
        initSession();
        message.reply(new JsonObject());
    }

    private void assignmentEvent(Message<JsonObject> message) {
        log.debug("AssignmentEvent - received message: " + message.body().toString());
        Timer.Sample s = Timer.start();
        IncidentAssignmentEvent incidentAssignmentEvent = new IncidentAssignmentEvent(message.body().getString("incidentId"),
                message.body().getBoolean("assignment"), new BigDecimal(message.body().getString("lat")), new BigDecimal(message.body().getString("lon")));
        ksession.insert(incidentAssignmentEvent);
        ksession.fireAllRules();
        s.stop(assignmentTimer);
    }

    private void incidentPriority(Message<JsonObject> message) {
        log.debug("IncidentPriority - received message: " + message.body().toString());
        Timer.Sample s = Timer.start();
        String incidentId = message.body().getString("incidentId");
        QueryResults results = ksession.getQueryResults("incidentPriority", incidentId);
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        Integer priority;
        boolean escalated;
        if (row == null) {
            priority = 0;
            escalated = false;
        } else {
            priority = ((IncidentPriority)row.get("incidentPriority")).getPriority();
            escalated = ((IncidentPriority)row.get("incidentPriority")).getEscalated();
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
        kieSessionDepth.set(results.size());
        Integer escalatedIncidents = Math.toIntExact(StreamSupport.stream(results.spliterator(), false).filter(incident -> {
            return ((IncidentPriority)incident.get("incidentPriority")).getEscalated();
        }).count());
        JsonObject jsonObject = new JsonObject().put("incidentId", incidentId).put("priority", priority)
                .put("average", average).put("incidents", results.size()).put("escalated", escalated).put("escalatedIncidents", escalatedIncidents);
        message.reply(jsonObject);
        s.stop(queryTimer);
    }

    /**
     * Retrieve all priority zones from the kie session
     * @param message a JsonArray containing the priority zones
     */
    private void priorityZones(Message<JsonObject> message) {
        log.debug("PriorityZones - received message: " + message.body().toString());
        QueryResults results = ksession.getQueryResults("priorityZones");
        JsonArray zones = new JsonArray();
        results.forEach(result -> {
            PriorityZone priorityZone = (PriorityZone) result.get("priorityZone");
            zones.add(new JsonObject()
                .put("id", priorityZone.getId())
                .put("lat", priorityZone.getLat().toString())
                .put("lon", priorityZone.getLon().toString())
                .put("radius", priorityZone.getRadius().toString()));
        });

        message.reply(zones);
    }

    /**
     * Add or update a priority zone
     * @param message the PriorityZoneApplicationEvent from kafka
     */
    private void priorityZone(Message<JsonObject> message) {
        log.debug("PriorityZoneApplicationEvent - received message {}", message.body());
        String id = message.body().getString("id");
        BigDecimal lat = new BigDecimal(message.body().getString("lat"));
        BigDecimal lon = new BigDecimal(message.body().getString("lon"));
        BigDecimal radius = new BigDecimal(message.body().getString("radius"));

        PriorityZone priorityZone = new PriorityZone(id, lat, lon, radius);
        PriorityZoneApplicationEvent event = new PriorityZoneApplicationEvent(priorityZone);
        ksession.insert(event);
        ksession.fireAllRules();
    }

    private void clearPriorityZones(Message<JsonObject> message) {
        log.debug("PriorityZoneClearEvent - received message {}", message.body());
        ksession.insert(new PriorityZoneClearEvent());
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
}
