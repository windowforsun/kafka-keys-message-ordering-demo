package com.windowforsun.kafka.keys.consumer;

import com.windowforsun.kafka.keys.event.DemoInboundKey;
import com.windowforsun.kafka.keys.event.DemoInboundPayload;
import com.windowforsun.kafka.keys.service.DemoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DemoConsumer {
    private final DemoService demoService;

    @KafkaListener(
            topics = "demo-inbound-topic",
            groupId = "demo-consumer-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId,
                       @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY)DemoInboundKey key,
                       @Payload final DemoInboundPayload payload) {
        log.info("Received message - key id: " + key.getId() + " - partition: " + partitionId + " - payload: " + payload);
        try {
            this.demoService.process(key, payload);
        } catch (Exception e) {
            log.error("Error processing message: " + e.getMessage());
        }
    }
}
