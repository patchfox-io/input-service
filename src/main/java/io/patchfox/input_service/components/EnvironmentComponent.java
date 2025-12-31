package io.patchfox.input_service.components;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.Getter;


@Getter
@Component
public class EnvironmentComponent {
    
    @Value("${spring.application.name}")
    String serviceName;

    @Value("${spring.kafka.request-topic}")
    String kafkaRequestTopicName;

    @Value("${spring.kafka.response-topic}")
    String kafkaResponseTopicName;

    @Value("${spring.kafka.request.client-id-prefix}")
    String kafkaRequestClientIdPrefix;

    @Value("${spring.kafka.response.client-id-prefix}")
    String kafkaResponseClientIdPrefix;

    @Value("${spring.kafka.group-name}")
    String kafkaGroupName;

    //

    @Value("${patchfox.feature.mq-controller}")
    private boolean isMqControllerEnabled;

    @Value("${patchfox.feature.mq-controller.secret}")
    private String mqControllerSecret;

    @Value("${patchfox.feature.mq-controller.valid-topics}")
    private String validTopicsAsString;

    @Value("${patchfox.expected.domain}")
    private String expectedDomain;

}
