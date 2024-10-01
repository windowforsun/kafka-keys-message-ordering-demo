package com.windowforsun.kafka.keys.integration;

import com.windowforsun.kafka.keys.DemoConfig;
import com.windowforsun.kafka.keys.event.DemoInboundKey;
import com.windowforsun.kafka.keys.event.DemoInboundPayload;
import com.windowforsun.kafka.keys.event.DemoOutboundKey;
import com.windowforsun.kafka.keys.event.DemoOutboundPayload;
import com.windowforsun.kafka.keys.util.TestData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

@Slf4j
@SpringBootTest(classes = DemoConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(
        controlledShutdown = true,
        topics = {"demo-inbound-topic", "demo-outbound-topic", "noKey"},
        partitions = 10
)
public class KafkaIntegrationTest {
    private static final String DEMO_INBOUND_TEST_TOPIC = "demo-inbound-topic";
    private static final String DEMO_OUTBOUND_TEST_TOPIC = "demo-outbound-topic";

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private KafkaTestListener testReceiver;

    @Configuration
    static class TestConfig {
        @Bean
        public KafkaTestListener testReceiver() {
            return new KafkaTestListener();
        }
    }

    public static class KafkaTestListener {
        AtomicInteger counter = new AtomicInteger(0);
        List<ImmutablePair<DemoOutboundKey, DemoOutboundPayload>> keyedMessages = new ArrayList<>();
        Map<DemoOutboundKey, Set<Integer>> partitionsByKey = new HashMap<>();
        Map<DemoOutboundKey, List<ImmutablePair<Integer, Integer>>> messagesByKey = new HashMap<>();

        @KafkaListener(
                groupId = "KafkaIntegrationTest",
                topics = DEMO_OUTBOUND_TEST_TOPIC,
                autoStartup = "true"
        )
        void receive(@Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId,
                     @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) DemoOutboundKey key,
                     @Payload final DemoOutboundPayload payload
        ) {
            log.debug("KafkaTestListener - Received message: partition: {} - key : {} - outbound data : {}",
                    partitionId,
                    key,
                    payload);

//            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            this.keyedMessages.add(ImmutablePair.of(key, payload));

            Set<Integer> partitions = this.partitionsByKey.get(key);

            if(partitions == null) {
                partitions = new HashSet<>();
                this.partitionsByKey.put(key, partitions);
            }
            partitions.add(partitionId);

            List<ImmutablePair<Integer, Integer>> messages;

            if(!this.messagesByKey.containsKey(key)) {
                messages = new ArrayList<>();
                this.messagesByKey.put(key, messages);
            } else {
                messages = this.messagesByKey.get(key);
            }
            messages.add(ImmutablePair.of(partitionId, payload.getSequenceNumber()));

            this.counter.incrementAndGet();
        }


        @KafkaListener(
                groupId = "KafkaIntegrationTest",
                topics = "noKey",
                autoStartup = "true"
        )
        public void noKeyReceive(@Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId,
                                 @Payload final DemoOutboundPayload payload) {
            log.debug("KafkaTestListener - Received message: partition: {} - key : - outbound data : {}",
                    partitionId,
                    payload);
        }
    }

    @BeforeEach
    public void setUp() {
        this.registry.getListenerContainers()
                .stream()
                .forEach(container -> ContainerTestUtils.waitForAssignment(container, this.embeddedKafkaBroker.getPartitionsPerTopic()));
        this.testReceiver.counter.set(0);
        this.testReceiver.keyedMessages = new ArrayList<>();
        this.testReceiver.partitionsByKey = new HashMap<>();
        this.testReceiver.messagesByKey = new HashMap<>();
    }

    @Test
    public void testSingleEvent() throws Exception {
        Integer keyId = RandomUtils.nextInt(1, 6);
        DemoInboundKey testKey = TestData.buildDemoInboundKey(keyId);
        DemoInboundPayload testPayload = TestData.buildDemoInboundPayload(1);

        this.kafkaTemplate.send(DEMO_INBOUND_TEST_TOPIC, testKey, testPayload).get();

        Awaitility.await().atMost(1, TimeUnit.SECONDS)
                .pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiver.counter::get, is(1));

        assertThat(this.testReceiver.keyedMessages.size(), is(1));
        assertThat(this.testReceiver.keyedMessages.get(0).getLeft().getId(), is(keyId));
        assertThat(this.testReceiver.keyedMessages.get(0).getRight().getOutboundData(), is("Processed:" + testPayload.getInboundData()));
    }

    @Test
    public void testKeyOrdering() throws Exception {
        int totalMessages = 10_000;
        for(int i = 0; i < totalMessages; i++) {
            Integer keyId = RandomUtils.nextInt(1, 6);
            DemoInboundKey testKey = TestData.buildDemoInboundKey(keyId);
            DemoInboundPayload testPayload = TestData.buildDemoInboundPayload(i + 1);
            this.kafkaTemplate.send(DEMO_INBOUND_TEST_TOPIC, testKey, testPayload);
        }

        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiver.counter::get, is(totalMessages));

        // each key mapped by one partition
        this.testReceiver.partitionsByKey
                .entrySet()
                .stream()
                .forEach(map -> assertThat(map.getValue().size(), equalTo(1)));
        this.testReceiver.messagesByKey
                .keySet()
                .stream()
                .forEach(key -> {
                    List<ImmutablePair<Integer, Integer>> messages = this.testReceiver.messagesByKey.get(key);

                    // same key messages should same partition
                    assertThat(messages.stream().map(map -> map.getLeft()).distinct().count(), equalTo(1L));

                    // same key message's sequence should ascending
                    messages.stream()
                            .map(map -> map.getRight())
                            .reduce((sequenceCurrent, sequenceNext) -> {
                                log.debug("sequenceCurrent : {} - sequenceNext : {}", sequenceCurrent, sequenceNext);
                                assertThat(sequenceCurrent, lessThan(sequenceNext));

                                return sequenceNext;
                            });
                });
    }

    @Test
    public void testNoKeyNotOrdering() throws Exception {
        int totalMessages = 10;
        for(int i = 0; i < totalMessages; i++) {
            DemoInboundPayload testPayload = TestData.buildDemoInboundPayload(i + 1);
            this.kafkaTemplate.send("noKey", testPayload);
        }

        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiver.counter::get, is(totalMessages));
    }
}
