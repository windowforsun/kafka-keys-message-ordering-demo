package com.windowforsun.kafka.keys.service;

import com.windowforsun.kafka.keys.event.DemoInboundKey;
import com.windowforsun.kafka.keys.event.DemoInboundPayload;
import com.windowforsun.kafka.keys.event.DemoOutboundKey;
import com.windowforsun.kafka.keys.event.DemoOutboundPayload;
import com.windowforsun.kafka.keys.producer.DemoProducer;
import com.windowforsun.kafka.keys.util.TestData;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class DemoServiceTest {
    private DemoProducer demoProducerMock;
    private DemoService demoService;

    @BeforeEach
    public void setUp() {
        this.demoProducerMock = mock(DemoProducer.class);
        this.demoService = new DemoService(this.demoProducerMock);
    }

    @Test
    public void testProcesS() {
        DemoInboundKey testKey = TestData.buildDemoInboundKey(RandomUtils.nextInt(1, 6));
        DemoInboundPayload testPayload = TestData.buildDemoInboundPayload(1);

        this.demoService.process(testKey, testPayload);

        verify(this.demoProducerMock, times(1))
                .sendMessage(any(DemoOutboundKey.class), any(DemoOutboundPayload.class));
    }
}
