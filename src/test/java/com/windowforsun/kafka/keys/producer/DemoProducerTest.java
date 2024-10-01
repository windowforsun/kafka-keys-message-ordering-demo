package com.windowforsun.kafka.keys.producer;

import com.windowforsun.kafka.keys.event.DemoOutboundKey;
import com.windowforsun.kafka.keys.event.DemoOutboundPayload;
import com.windowforsun.kafka.keys.properties.DemoProperties;
import com.windowforsun.kafka.keys.util.TestData;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

public class DemoProducerTest {
    private DemoProperties propertiesMock;
    private KafkaTemplate kafkaTemplateMock;
    private DemoProducer demoProducer;

    @BeforeEach
    public void setUp() {
        this.propertiesMock = mock(DemoProperties.class);
        this.kafkaTemplateMock = mock(KafkaTemplate.class);
        this.demoProducer = new DemoProducer(this.propertiesMock, this.kafkaTemplateMock);
    }

    @Test
    public void testSendMessage_Success() {
        DemoOutboundKey testKey = TestData.buildDemoOutboundKey(RandomUtils.nextInt(1, 6));
        DemoOutboundPayload testPayload = TestData.buildDemoOutboundPayload(1);
        String topic = "test-outbound-topic";

        when(this.propertiesMock.getOutboundTopic()).thenReturn(topic);

        this.demoProducer.sendMessage(testKey, testPayload);

        verify(this.kafkaTemplateMock, times(1)).send(topic, testKey, testPayload);
    }

    @Test
    public void testSendMessage_ExceptionOnSend() {
        DemoOutboundKey testKey = TestData.buildDemoOutboundKey(RandomUtils.nextInt(1, 6));
        DemoOutboundPayload testPayload = TestData.buildDemoOutboundPayload(1);
        String topic = "test-outbound-topic";

        when(this.propertiesMock.getOutboundTopic()).thenReturn(topic);
        doThrow(new RuntimeException("Kafka send fail"))
                .when(this.kafkaTemplateMock)
                .send(topic, testKey, testPayload);

        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            this.demoProducer.sendMessage(testKey, testPayload);
        });

        verify(this.kafkaTemplateMock, times(1)).send(topic, testKey, testPayload);
        assertThat(exception.getMessage(), is("Error sending message to topic " + topic));
    }
}
