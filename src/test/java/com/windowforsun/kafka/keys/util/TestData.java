package com.windowforsun.kafka.keys.util;

import com.windowforsun.kafka.keys.event.DemoInboundKey;
import com.windowforsun.kafka.keys.event.DemoInboundPayload;
import com.windowforsun.kafka.keys.event.DemoOutboundKey;
import com.windowforsun.kafka.keys.event.DemoOutboundPayload;

public class TestData {
    public static String INBOUND_DATA = "inbound event data";
    public static String OUTBOUND_DATA = "outbound event data";

    public static DemoInboundKey buildDemoInboundKey(Integer id) {
        return DemoInboundKey.builder()
                .id(id)
                .build();
    }

    public static DemoInboundPayload buildDemoInboundPayload(Integer sequenceNumber) {
        return DemoInboundPayload.builder()
                .inboundData(INBOUND_DATA)
                .sequenceNumber(sequenceNumber)
                .build();
    }

    public static DemoOutboundKey buildDemoOutboundKey(Integer id) {
        return DemoOutboundKey.builder()
                .id(id)
                .build();
    }

    public static DemoOutboundPayload buildDemoOutboundPayload(Integer sequenceNumber) {
        return DemoOutboundPayload.builder()
                .outboundData(OUTBOUND_DATA)
                .sequenceNumber(sequenceNumber)
                .build();
    }
}
