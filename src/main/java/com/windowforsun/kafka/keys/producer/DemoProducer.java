package com.windowforsun.kafka.keys.producer;

import com.windowforsun.kafka.keys.event.DemoOutboundKey;
import com.windowforsun.kafka.keys.event.DemoOutboundPayload;
import com.windowforsun.kafka.keys.properties.DemoProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DemoProducer {
    private final DemoProperties properties;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public void sendMessage(DemoOutboundKey key, DemoOutboundPayload payload) {
        try {
            this.kafkaTemplate.send(this.properties.getOutboundTopic(), key, payload);
            log.info("Emitted message - key: " + key + " - payload: " + payload.getOutboundData());
        } catch (Exception e) {
            String message = "Error sending message to topic " + this.properties.getOutboundTopic();
            log.error(message);
            throw new RuntimeException(message, e);
        }
    }
}
