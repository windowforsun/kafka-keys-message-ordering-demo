package com.windowforsun.kafka.keys.service;

import com.windowforsun.kafka.keys.event.DemoInboundKey;
import com.windowforsun.kafka.keys.event.DemoInboundPayload;
import com.windowforsun.kafka.keys.event.DemoOutboundKey;
import com.windowforsun.kafka.keys.event.DemoOutboundPayload;
import com.windowforsun.kafka.keys.producer.DemoProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DemoService {
    private final DemoProducer demoProducer;

    public void process(DemoInboundKey key, DemoInboundPayload payload) {
        DemoOutboundPayload outboundPayload = DemoOutboundPayload.builder()
                .outboundData("Processed:" + payload.getInboundData())
                .sequenceNumber(payload.getSequenceNumber())
                .build();

        DemoOutboundKey outboundKey = DemoOutboundKey.builder()
                .id(key.getId())
                .build();

        this.demoProducer.sendMessage(outboundKey, outboundPayload);
    }
}
