package com.windowforsun.kafka.keys.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DemoOutboundPayload {
    private String outboundData;
    private Integer sequenceNumber;
}
