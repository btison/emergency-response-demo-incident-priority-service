package com.redhat.emergency.response.incident.priority.rules;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.math.BigDecimal;
import java.util.stream.StreamSupport;

import com.redhat.emergency.response.incident.priority.rules.model.AveragePriority;
import com.redhat.emergency.response.incident.priority.rules.model.IncidentAssignmentEvent;
import com.redhat.emergency.response.incident.priority.rules.model.IncidentPriority;
import com.redhat.emergency.response.incident.priority.rules.model.PriorityZone;
import com.redhat.emergency.response.incident.priority.rules.model.PriorityZoneApplicationEvent;
import com.redhat.emergency.response.incident.priority.rules.model.PriorityZoneClearEvent;

import org.drools.core.io.impl.ClassPathResource;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kie.api.KieBase;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.Message;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.io.Resource;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.QueryResults;
import org.kie.api.runtime.rule.QueryResultsRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriorityRulesTest {

    private static KieBase kieBase;

    private KieSession session;

    @BeforeClass
    public static void init() {
        kieBase = setupKieBase("com/redhat/emergency/response/incident/priority/rules/priority_rules.drl");
    }

    @Before
    public void setupTest() {
        session = kieBase.newKieSession();
        Logger logger = LoggerFactory.getLogger("PriorityRules");
        Integer priorityZoneUpsurge = 50;
        session.setGlobal("logger", logger);
        session.setGlobal("priorityZoneUpsurge", priorityZoneUpsurge);
    }

    @After
    public void teardownTest() {
        if (session != null) {
            session.dispose();
        }
    }

    @Test
    public void testIncidentPriorityFact() {
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("incidentPriority", "incident123");
        assertThat(results.size(), equalTo(1));
    }

    @Test
    public void testIncreaseIncidentPriority() {
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("incidentPriority", "incident123");
        assertThat(results.size(), equalTo(1));
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        assertThat(row, notNullValue());
        assertThat(row.get("incidentPriority"), notNullValue());
        assertThat(row.get("incidentPriority"), is(instanceOf(IncidentPriority.class)));
        assertThat(((IncidentPriority)row.get("incidentPriority")).getPriority(), equalTo(1));
        QueryResults incidents = session.getQueryResults("incidents");
        assertThat(incidents.size(), equalTo(1));
    }

    @Test
    public void testRetractIncidentPriority() {
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("incident123", true, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("incidentPriority", "incident123");
        assertThat(results.size(), equalTo(0));
        QueryResults incidents = session.getQueryResults("incidents");
        assertThat(incidents.size(), equalTo(0));
    }

    @Test
    public void testAveragePriority1() {
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("averagePriority");
        assertThat(results.size(), equalTo(1));
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        assertThat(row, notNullValue());
        assertThat(row.get("averagePriority"), notNullValue());
        assertThat(row.get("averagePriority"), is(instanceOf(AveragePriority.class)));
        assertThat(((AveragePriority)row.get("averagePriority")).getResult(), equalTo(1.0));
        QueryResults incidents = session.getQueryResults("incidents");
        assertThat(incidents.size(), equalTo(1));
    }

    @Test
    public void testAveragePriority2() {
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("averagePriority");
        assertThat(results.size(), equalTo(1));
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        assertThat(row, notNullValue());
        assertThat(row.get("averagePriority"), notNullValue());
        assertThat(row.get("averagePriority"), is(instanceOf(AveragePriority.class)));
        assertThat(((AveragePriority)row.get("averagePriority")).getResult(), equalTo(2.0));
        QueryResults incidents = session.getQueryResults("incidents");
        assertThat(incidents.size(), equalTo(1));
    }

    @Test
    public void testAveragePriority3() {
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("incident123", true, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("averagePriority");
        assertThat(results.size(), equalTo(1));
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        assertThat(row, notNullValue());
        assertThat(row.get("averagePriority"), notNullValue());
        assertThat(row.get("averagePriority"), is(instanceOf(AveragePriority.class)));
        assertThat(((AveragePriority)row.get("averagePriority")).getResult(), equalTo(0.0));
        QueryResults incidents = session.getQueryResults("incidents");
        assertThat(incidents.size(), equalTo(0));
    }

    @Test
    public void testAveragePriority4() {
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("incident123", true, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("averagePriority");
        assertThat(results.size(), equalTo(1));
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        assertThat(row, notNullValue());
        assertThat(row.get("averagePriority"), notNullValue());
        assertThat(row.get("averagePriority"), is(instanceOf(AveragePriority.class)));
        assertThat(((AveragePriority)row.get("averagePriority")).getResult(), equalTo(0.0));
        QueryResults incidents = session.getQueryResults("incidents");
        assertThat(incidents.size(), equalTo(0));
    }

    @Test
    public void testAveragePriority5() {
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("incident456", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("averagePriority");
        assertThat(results.size(), equalTo(1));
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        assertThat(row, notNullValue());
        assertThat(row.get("averagePriority"), notNullValue());
        assertThat(row.get("averagePriority"), is(instanceOf(AveragePriority.class)));
        assertThat(((AveragePriority)row.get("averagePriority")).getResult(), equalTo(1.0));
        QueryResults incidents = session.getQueryResults("incidents");
        assertThat(incidents.size(), equalTo(2));
    }

    @Test
    public void testAveragePriority6() {
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("incident456", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("incident456", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("averagePriority");
        assertThat(results.size(), equalTo(1));
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        assertThat(row, notNullValue());
        assertThat(row.get("averagePriority"), notNullValue());
        assertThat(row.get("averagePriority"), is(instanceOf(AveragePriority.class)));
        assertThat(((AveragePriority)row.get("averagePriority")).getResult(), equalTo(1.5));
        QueryResults incidents = session.getQueryResults("incidents");
        assertThat(incidents.size(), equalTo(2));
    }

    @Test
    public void testAveragePriority7() {
        session.insert(new IncidentAssignmentEvent("incident123", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("incident456", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("incident456", false, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("incident123", true, BigDecimal.ZERO, BigDecimal.ZERO));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("averagePriority");
        assertThat(results.size(), equalTo(1));
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        assertThat(row, notNullValue());
        assertThat(row.get("averagePriority"), notNullValue());
        assertThat(row.get("averagePriority"), is(instanceOf(AveragePriority.class)));
        assertThat(((AveragePriority)row.get("averagePriority")).getResult(), equalTo(2.0));
        QueryResults incidents = session.getQueryResults("incidents");
        assertThat(incidents.size(), equalTo(1));
    }

    @Test
    public void testPriorityZoneAddedBeforeIncident() {
        PriorityZone priorityZone = new PriorityZone("pz123", new BigDecimal("34.19439"), new BigDecimal("-77.81453"), new BigDecimal(6));
        session.insert(new PriorityZoneApplicationEvent(priorityZone));
        session.fireAllRules();
        PriorityZone priorityZone2 = new PriorityZone("pz1234", new BigDecimal(0), new BigDecimal(0), new BigDecimal(6));
        session.insert(new PriorityZoneApplicationEvent(priorityZone2));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("3a64e1f5-848c-42af-bc8c-abce650d4e46", false,  new BigDecimal("34.19439"), new BigDecimal("-77.81453")));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("incidentPriority", "3a64e1f5-848c-42af-bc8c-abce650d4e46");
        assertThat(results.size(), equalTo(1));
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        assertThat(row, notNullValue());
        IncidentPriority priority = (IncidentPriority)row.get("incidentPriority");
        assertThat(priority.getEscalated(), equalTo(true));
        assertThat(priority.getPriority(), equalTo(51));
    }


    @Test
    public void testPriorityZoneAddedAfterIncident() {
        session.insert(new IncidentAssignmentEvent("3a64e1f5-848c-42af-bc8c-abce650d4e44", false, new BigDecimal(0), new BigDecimal("-77.81453")));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("3a64e1f5-848c-42af-bc8c-abce650d4e46", false, new BigDecimal("34.19439"), new BigDecimal("-77.81453")));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("3a64e1f5-848c-42af-bc8c-abce650d4e45", false, new BigDecimal(0), new BigDecimal("-77.81453")));
        session.fireAllRules();
        PriorityZone priorityZone = new PriorityZone("pz123", new BigDecimal("34.19439"), new BigDecimal("-77.81453"), new BigDecimal(6));
        session.insert(new PriorityZoneApplicationEvent(priorityZone));
        session.fireAllRules();
        QueryResults results = session.getQueryResults("incidentPriority", "3a64e1f5-848c-42af-bc8c-abce650d4e46");
        assertThat(results.size(), equalTo(1));
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        assertThat(row, notNullValue());
        IncidentPriority priority = (IncidentPriority)row.get("incidentPriority");
        assertThat(priority.getEscalated(), equalTo(true));
        assertThat(priority.getPriority(), equalTo(51));
    }

    @Test
    public void testPriorityZoneClearEvent() {
        PriorityZone priorityZone = new PriorityZone("pz123", new BigDecimal("34.19439"), new BigDecimal("-77.81453"), new BigDecimal(6));
        session.insert(new PriorityZoneApplicationEvent(priorityZone));
        session.fireAllRules();
        PriorityZone priorityZone2 = new PriorityZone("pz1234", new BigDecimal("34.19439"), new BigDecimal("-77.81453"), new BigDecimal(6));
        session.insert(new PriorityZoneApplicationEvent(priorityZone2));
        session.fireAllRules();
        session.insert(new PriorityZoneClearEvent());
        session.fireAllRules();
        QueryResults results = session.getQueryResults("priorityZones");
        assertThat(results.size(), equalTo(0));
    }

    @Test
    public void testPriorityZoneDeEscalation() {
        PriorityZone priorityZone = new PriorityZone("pz123", new BigDecimal("34.19439"), new BigDecimal("-77.81453"), new BigDecimal(6));
        session.insert(new PriorityZoneApplicationEvent(priorityZone));
        session.fireAllRules();
        session.insert(new IncidentAssignmentEvent("3a64e1f5-848c-42af-bc8c-abce650d4e46", false, new BigDecimal("34.19439"), new BigDecimal("-77.81453")));
        session.fireAllRules();

        //make sure incident gets escalated
        QueryResults results = session.getQueryResults("incidentPriority", "3a64e1f5-848c-42af-bc8c-abce650d4e46");
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        IncidentPriority priority = (IncidentPriority)row.get("incidentPriority");
        assertThat(priority.getEscalated(), equalTo(true));
        assertThat(priority.getPriority(), equalTo(51));

        //change the location of the priority zone
        PriorityZone priorityZoneUpdate = new PriorityZone("pz123", new BigDecimal(0), new BigDecimal(0), new BigDecimal(6));
        session.insert(new PriorityZoneApplicationEvent(priorityZoneUpdate));
        session.fireAllRules();

        //make sure incident gets de-escalated
        QueryResults resultsUpdate = session.getQueryResults("incidentPriority", "3a64e1f5-848c-42af-bc8c-abce650d4e46");
        QueryResultsRow rowUpdate = StreamSupport.stream(resultsUpdate.spliterator(), false).findFirst().orElse(null);
        IncidentPriority priorityUpdate = (IncidentPriority)rowUpdate.get("incidentPriority");
        assertThat(priorityUpdate.getEscalated(), equalTo(false));
        assertThat(priorityUpdate.getPriority(), equalTo(1));
    }

    @Test
    public void testSkipDeEscalationWhenInMultiplePriorityZones() {
        //Add two priority zones in the same spot
        PriorityZone priorityZone = new PriorityZone("pz123", new BigDecimal("34.19439"), new BigDecimal("-77.81453"), new BigDecimal(6));
        session.insert(new PriorityZoneApplicationEvent(priorityZone));
        session.fireAllRules();
        PriorityZone priorityZone2 = new PriorityZone("pz1234", new BigDecimal("34.19439"), new BigDecimal("-77.81453"), new BigDecimal(6));
        session.insert(new PriorityZoneApplicationEvent(priorityZone2));
        session.fireAllRules();

        //Add an incident that falls in both
        session.insert(new IncidentAssignmentEvent("3a64e1f5-848c-42af-bc8c-abce650d4e46", false, new BigDecimal("34.19439"), new BigDecimal("-77.81453")));
        session.fireAllRules();

        //make sure incident gets escalated
        QueryResults results = session.getQueryResults("incidentPriority", "3a64e1f5-848c-42af-bc8c-abce650d4e46");
        QueryResultsRow row = StreamSupport.stream(results.spliterator(), false).findFirst().orElse(null);
        IncidentPriority priority = (IncidentPriority)row.get("incidentPriority");
        assertThat(priority.getEscalated(), equalTo(true));
        assertThat(priority.getPriority(), equalTo(51));

        //change the location of one of the priority zones
        PriorityZone priorityZoneUpdate = new PriorityZone("pz123", new BigDecimal(0), new BigDecimal(0), new BigDecimal(6));
        session.insert(new PriorityZoneApplicationEvent(priorityZoneUpdate));
        session.fireAllRules();

        //make sure incident is still escalated
        QueryResults results2 = session.getQueryResults("incidentPriority", "3a64e1f5-848c-42af-bc8c-abce650d4e46");
        QueryResultsRow row2 = StreamSupport.stream(results2.spliterator(), false).findFirst().orElse(null);
        IncidentPriority priority2 = (IncidentPriority)row2.get("incidentPriority");
        assertThat(priority2.getEscalated(), equalTo(true));
        assertThat(priority2.getPriority(), equalTo(51));
    }

    private static KieBase setupKieBase(String... resources) {
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

    private static void hasErrors(KieBuilder kbuilder) {
        if (kbuilder.getResults().hasMessages(Message.Level.ERROR)) {
            throw new RuntimeException("Build errors\n" + kbuilder.getResults().toString());
        }
    }

}
