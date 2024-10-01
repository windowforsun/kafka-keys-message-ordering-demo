package com.windowforsun.kafka.keys.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;

@Configuration
@ConfigurationProperties("kafkademo")
@Data
@Validated
public class DemoProperties {
    @NotNull
    private String outboundTopic;
}
