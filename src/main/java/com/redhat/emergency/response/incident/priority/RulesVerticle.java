package com.redhat.emergency.response.incident.priority;

import java.util.stream.StreamSupport;

import com.redhat.emergency.response.incident.priority.rules.model.AveragePriority;
import com.redhat.emergency.response.incident.priority.rules.model.IncidentAssignmentEvent;
import com.redhat.emergency.response.incident.priority.rules.model.IncidentPriority;
import com.redhat.cajun.navy.incident.priority.tracing.TracingKafkaUtils;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
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

    private Tracer tracer;

    @Override
    public Completable rxStart() {
        
        return Completable.fromMaybe(vertx.<Void>rxExecuteBlocking(future -> {
            try {
                tracer = GlobalTracer.get();
                kbase = setupKieBase("com/redhat/emergency/response/incident/priority/rules/priority_rules.drl");
                initSession();

                vertx.eventBus().consumer("incident-assignment-event", this::assignmentEvent);
                vertx.eventBus().consumer("incident-priority", this::incidentPriority);
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

        Span span = TracingKafkaUtils.buildChildSpan("incidentPriorityAssignment", message, tracer);

        try {
            IncidentAssignmentEvent incidentAssignmentEvent = new IncidentAssignmentEvent(message.body().getString("incidentId"),
                    message.body().getBoolean("assignment"));
            ksession.insert(incidentAssignmentEvent);
            ksession.fireAllRules();
        } finally {
            if (span != null) {
                span.finish();
            }
        }
    }

    private void incidentPriority(Message<JsonObject> message) {
        log.debug("IncidentPriority - received message: " + message.body().toString());

        Span span = TracingKafkaUtils.buildChildSpan("getIncidentPriority", message, tracer);

        try {
            String incidentId = message.body().getString("incidentId");
            QueryResults results = ksession.getQueryResults("incidentPriority", incidentId);
            QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
            Integer priority;
            if (row == null) {
                priority = 0;
            } else {
                priority = ((IncidentPriority) row.get("incidentPriority")).getPriority();
            }

            results = ksession.getQueryResults("averagePriority");
            row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
            Double average;
            if (row == null) {
                average = 0.0;
            } else {
                average = ((AveragePriority) row.get("averagePriority")).getAveragePriority();
            }

            results = ksession.getQueryResults("incidents");
            JsonObject jsonObject = new JsonObject().put("incidentId", incidentId).put("priority", priority)
                    .put("average", average).put("incidents", results.size());
            message.reply(jsonObject);
        } finally {
            if (span != null) {
                span.finish();
            }
        }
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
