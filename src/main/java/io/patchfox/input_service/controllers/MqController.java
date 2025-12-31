package io.patchfox.input_service.controllers;


import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.Map;
import java.util.Optional;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;


import io.patchfox.input_service.components.EnvironmentComponent;
import io.patchfox.input_service.kafka.KafkaBeans;
import io.patchfox.package_utils.json.ApiRequest;
import io.patchfox.package_utils.json.ApiResponse;
import lombok.extern.slf4j.Slf4j;


@RestController
@Slf4j
public class MqController {

    public static final String API_PATH_PREFIX = "/api/v1";
    public static final String INPUT_PATH = API_PATH_PREFIX + "/input";
    public static final String INPUT_MQ_PATH = INPUT_PATH + "/mq";
    public static final String POST_INPUT_MQ_PATH_SIGNATURE = "POST_" + INPUT_MQ_PATH;

    public static final String HEADER_SECRET_KEY = "PF-FF-MQ_CONTROLLER-SECRET";
    public static final String HEADER_REQUEST_TOPIC_KEY = "PF-FF-REQUEST_TOPIC";

    @Autowired
    EnvironmentComponent env;

    @Autowired
    KafkaBeans kafka;

    @PostMapping(
        value = INPUT_MQ_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> inputGetHandler(
        @RequestAttribute UUID txid,
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestHeader Map<String, String> allHeaders,
        @RequestBody Optional<ApiRequest> apiRequestOptional
    ) {
        
        var validTopics = Arrays.asList(env.getValidTopicsAsString().split(","));

        // check ApiRequest first because we have to do stuff to it to make it valid 
        // caller won't and shouldn't be adding txid and requestReceivedAt values... 
        if ( 
            !apiRequestOptional.isPresent() 
            || allHeaders.get(HEADER_SECRET_KEY.toLowerCase()) == null
            || allHeaders.get(HEADER_REQUEST_TOPIC_KEY.toLowerCase()) == null
        ) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        var apiRequest = apiRequestOptional.get();
        var secret = allHeaders.get(HEADER_SECRET_KEY.toLowerCase());
        var topic = allHeaders.get(HEADER_REQUEST_TOPIC_KEY.toLowerCase());

        apiRequest.setTxid(txid);
        apiRequest.setResponseTopicName(env.getKafkaResponseTopicName());

        // now run the gauntlet of remaining validation checks 
        if ( !env.isMqControllerEnabled()
            // bad secret key 
            || secret == null
            || !secret.equals(env.getMqControllerSecret())
            // bad request topic
            || topic == null
            || !validTopics.contains(topic)
            // bad api request
            || !apiRequest.isValidForKafka()
        ) { 
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        // if we're here we're gtg 
        kafka.makeRequest(topic, apiRequest);
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
