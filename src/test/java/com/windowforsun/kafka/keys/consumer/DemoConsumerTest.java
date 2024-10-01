package com.windowforsun.kafka.keys.consumer;

import com.windowforsun.kafka.keys.event.DemoInboundKey;
import com.windowforsun.kafka.keys.event.DemoInboundPayload;
import com.windowforsun.kafka.keys.service.DemoService;
import com.windowforsun.kafka.keys.util.TestData;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class DemoConsumerTest {
    private DemoService demoServiceMock;
    private DemoConsumer demoConsumer;

    @BeforeEach
    public void setUp() {
        this.demoServiceMock = mock(DemoService.class);
        this.demoConsumer = new DemoConsumer(this.demoServiceMock);
    }

    @Test
    public void testListen_Success() {
        DemoInboundKey testKey = TestData.buildDemoInboundKey(RandomUtils.nextInt(1, 6));
        DemoInboundPayload testPayload = TestData.buildDemoInboundPayload(1);

        this.demoConsumer.listen(0, testKey, testPayload);

        verify(this.demoServiceMock, times(1)).process(testKey, testPayload);
    }

    @Test
    public void testListen_ServiceThrowsException() {
        DemoInboundKey testKey = TestData.buildDemoInboundKey(RandomUtils.nextInt(1, 6));
        DemoInboundPayload testPayload = TestData.buildDemoInboundPayload(1);

        doThrow(new RuntimeException("Service failure"))
                .when(this.demoServiceMock)
                .process(testKey, testPayload);

        this.demoConsumer.listen(0, testKey, testPayload);

        verify(this.demoServiceMock, times(1)).process(testKey, testPayload);
    }
}
