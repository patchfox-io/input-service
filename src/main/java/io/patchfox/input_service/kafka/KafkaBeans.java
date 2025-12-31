package io.patchfox.input_service.kafka;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;

import org.apache.catalina.connector.Response;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;

import io.patchfox.input_service.components.EnvironmentComponent;
import io.patchfox.input_service.controllers.HealthCheckController;
import io.patchfox.input_service.controllers.InputController;
import io.patchfox.input_service.controllers.RestInfoController;
import io.patchfox.input_service.helpers.HibernateHelper;
import io.patchfox.input_service.repositories.DatasetRepository;
import io.patchfox.input_service.repositories.DatasourceEventRepository;
import io.patchfox.input_service.repositories.DatasourceRepository;
import io.patchfox.input_service.repositories.FindingDataRepository;
import io.patchfox.input_service.repositories.FindingReporterRepository;
import io.patchfox.input_service.repositories.FindingRepository;
import io.patchfox.input_service.repositories.PackageRepository;
import io.patchfox.input_service.services.RestInfoService;
import io.patchfox.package_utils.json.ApiRequest;
import io.patchfox.package_utils.json.ApiResponse;
import io.patchfox.package_utils.util.Pair;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class KafkaBeans {

    @Autowired
    private KafkaTemplate<String, ApiRequest> kafkaRequestTemplate;

    @Autowired
    private KafkaTemplate<String, ApiResponse> kafkaResponseTemplate;

    @Autowired 
    RestInfoService restInfoService;

    @Autowired
    ApplicationContext context;

    @Autowired
    EnvironmentComponent env;

    @Autowired
    HibernateHelper hibernateHelper;

    @Autowired
    DatasourceEventRepository datasourceEventRepository;

    @Autowired
    DatasourceRepository datasourceRepository;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    FindingReporterRepository findingReporterRepository;

    @Autowired
    FindingDataRepository findingDataRepository;

    @Autowired
    FindingRepository findingRepository;

    @Autowired
    PackageRepository packageRepository;



    //
    // create topics for other services to send and receive messages on 
    //

    @Bean
    public NewTopic serviceRequestTopic() {
        return TopicBuilder.name(env.getKafkaRequestTopicName())
                           // *!* you need at least as many partitions as you have consumers
                           // check "spring.kafka.listener.concurrency" in file application.properties 
                           .partitions(10)
                           .replicas(1)
                           .build();
    }

    @Bean
    public NewTopic serviceResponseTopic() {
        return TopicBuilder.name(env.getKafkaResponseTopicName())
                           // *!* you need at least as many partitions as you have consumers
                           // check "spring.kafka.listener.concurrency" in file application.properties 
                           .partitions(10)
                           .replicas(1)
                           .build();
    }


    //
    // create listeners for the topics this service will send and receive on.
    // note that the reason we're not using the "env" component here is because the Kafka annotations are fun in that 
    // they don't allow for strings that aren't constants. You HAVE to use the property placeholder directly if you want
    // to make the id and topic configurable by way of the application.yml file.
    //

    @KafkaListener(
        clientIdPrefix = "#'${spring.kafka.request.client-id-prefix}'",
        groupId = "#'${spring.kafka.group-name}'",
        topics = "#{'${spring.kafka.request-topic}'}",
        properties = {"spring.json.value.default.type=io.patchfox.package_utils.json.ApiRequest"}
    )
    public void listenToRequestTopic(ApiRequest apiRequest) throws Exception {
        log.info("received apiRequest message: {}", apiRequest);
        var now = ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
        var responseTopicName = apiRequest.getResponseTopicName();
        var txid = apiRequest.getTxid();
        var verb = apiRequest.getVerb();
        var resource = apiRequest.getUri();
        var resourceSignature = verb + "_" + resource.toString();
        try {
            var requestPair = new Pair<>(verb, resource);
            var handlerMethod = restInfoService.getHandlerFor(requestPair);
            var apiResponse = invokeMethod(txid, verb.toString(), resource.toString(), handlerMethod, now, apiRequest);
            apiResponse.setResponderName(env.getServiceName());
            apiResponse.setResponderResourceSignature(resourceSignature);
            kafkaResponseTemplate.send(responseTopicName, apiResponse);
        } catch (NullPointerException e) {
            var notFoundResponse = ApiResponse.builder()
                                              .responderName(env.getServiceName())
                                              .code(Response.SC_NOT_FOUND)
                                              .txid(txid)
                                              .requestReceivedAt(now.toString())
                                              .build();

            kafkaResponseTemplate.send(responseTopicName, notFoundResponse);
        } catch (Exception e) {
            log.error("exception was: ", e);
            var serverErrorResponse = ApiResponse.builder()
                                              .responderName(env.getServiceName())
                                              .code(Response.SC_INTERNAL_SERVER_ERROR)
                                              .txid(txid)
                                              .requestReceivedAt(now.toString())
                                              .build();

            kafkaResponseTemplate.send(responseTopicName, serverErrorResponse);
        }
    }

    @KafkaListener(
        clientIdPrefix = "#'${spring.kafka.response.client-id-prefix}'",
        groupId = "#'${spring.kafka.group-name}'",
        topics = "#{'${spring.kafka.response-topic}'}",
        properties = {"spring.json.value.default.type=io.patchfox.package_utils.json.ApiResponse"}
    )
    public void listenToResponseTopic(ApiResponse response) throws Exception {
        // here is where we inspect the response object and figure out what, if anything, we need to do next 
        log.info("received apiResponse message: {}", response);

        // switch(response.getResponderName()) {
        //     //
        //     // TODO move this monstrosity to its own method 
        //     //
        //     case "grype-service":
        //         // we're here because the input flow has sent an event out to the grype-service to get an oss report
        //         // for the SBOM the caller provided us. We need to serialize the packages and findings to db
        //         if (response.getResponderResourceSignature().equals("POST_/api/v1/grype")) {
        //             var httpStatus = HttpStatus.valueOf(response.getCode());
                    
        //             // something went wrong - set error state for all appropriate db records 
        //             if ( !httpStatus.is2xxSuccessful() ) {
        //                 log.error("something went wrong with grype oss job. marking event status as error");
        //                 hibernateHelper.recordErrorAndGetApiResponse(
        //                     HttpStatus.valueOf(response.getCode()).toString(),
        //                     response.getTxid(),
        //                     ZonedDateTime.parse(response.getRequestReceivedAt())
        //                 );
        //             }

        //             PackageWrapper wrappedPackage = ApiDataHelpers.getPojoFromDataMap(response.getData(), PackageWrapper.class);
        //             var packageData = wrappedPackage.getDependencyData();
        //             if ( !packageData.containsKey(PackageData.PackageDataType.OSS_REPORT) ) {
        //                 log.error("expected OSS report and did not find one. marking event status as error");
        //                 hibernateHelper.recordErrorAndGetApiResponse(
        //                     HttpStatus.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase()).toString(),
        //                     response.getTxid(),
        //                     ZonedDateTime.parse(response.getRequestReceivedAt())
        //                 );
        //             }

        //             for (var ossPackageData : packageData.get(PackageData.PackageDataType.OSS_REPORT)) {
        //                 var grypeOssReportPackageData = (GrypeOssReportPackageData)ossPackageData;
                        
        //                 @SuppressWarnings("unchecked")
        //                 var ossSummaries = (List<OssSummary>)(Object)grypeOssReportPackageData.getOssDependencyData()
        //                                                                                       .stream()
        //                                                                                       .map(x -> x.getData())
        //                                                                                       .toList();
   
        //                 for (var ossSummary : ossSummaries) {

        //                     //
        //                     // findingReporter
        //                     //

        //                     Set<FindingReporter> findingReporters = new HashSet<>(); 
        //                     for (var reporterName : ossSummary.getReporters()) {
        //                         var findingReporterRecord = FindingReporter.builder()
        //                                                                    .name(reporterName)
        //                                                                    .build();

        //                         var findingReporterRecords = findingReporterRepository.findByName(reporterName);
        //                         if (findingReporterRecords.size() > 1) {
        //                             log.error(
        //                                 "something has gone very wrong. reporter record name field {} should be " + 
        //                                 "unique but record count is: {}. system integrity compromised. " +
        //                                 "throwing exception...",
        //                                 reporterName,
        //                                 findingReporterRecords.size()
        //                             );      

        //                             throw new IllegalStateException();
        //                         } else if (findingReporterRecords.size() == 1) {
        //                             findingReporterRecord = findingReporterRecords.get(0);
        //                         } else {
        //                             findingReporterRecord = findingReporterRepository.save(findingReporterRecord);
        //                         }
                                
        //                         findingReporters.add(findingReporterRecord);
        //                     }
                            
        //                     //
        //                     // findingData
        //                     //

        //                     // will blow up if you try to save it at this point - needs finding record 
        //                     var requestReceivedAt = ZonedDateTime.parse(response.getRequestReceivedAt());

        //                     var findingDataRecord = FindingData.builder()
        //                                                        .identifier(ossSummary.getVulnerabilityId())
        //                                                        .severity(ossSummary.getSeverity().toString())
        //                                                        .description(ossSummary.getDescription())
        //                                                        .cpes(ossSummary.getCpes())
        //                                                        .reportedAt(requestReceivedAt)
        //                                                        .build();

        //                     // check to see if record already exists. if it does use that one 
        //                     var findingDataRecords = 
        //                         findingDataRepository.findByIdentifier(findingDataRecord.getIdentifier());

        //                     if (findingDataRecords.size() > 1) {
        //                         log.error(
        //                             "something went very wrong. FindingData.identifier should be unique in database " +
        //                             "and is not. received {} records for same identifier {}. System integrity " +
        //                             "may be compromised. Throwing exception...",
        //                             findingDataRecords.size(),
        //                             findingDataRecord.getIdentifier()
        //                         );

        //                         throw new IllegalStateException();
        //                     } else if (findingDataRecords.size() == 1) {
        //                         findingDataRecord = findingDataRecords.get(0);
        //                     } else {
        //                         findingDataRecord = findingDataRepository.save(findingDataRecord);
        //                     }


        //                     // 
        //                     // finding
        //                     //

        //                     // at this point in the flow we are guaranteed to have a package record corresponding 
        //                     // to this purl. the package table gets updated on receipt of caller payload. we're
        //                     // now in a callback flow where we're are enriching the data we already have. if 
        //                     // this blows up something really done did went' wrong. 
        //                     var purl = ossSummary.getPurl().getCoordinates();
        //                     var packageRecord = packageRepository.findByPurl(purl).get(0);

        //                     // if finding is null it's because we're looking at the findingData obj we just made and 
        //                     // not one we made before and stored in the database. 
        //                     if (findingDataRecord.getFinding() == null) {
        //                         // we have to persist the record to db before we populate it with foreign objs
        //                         var findingRecord = Finding.builder().build();
        //                         findingRecord = findingRepository.save(findingRecord);
                            

        //                         // add findingRecord to findingData, package, and findingReporter records
        //                         findingDataRecord.setFinding(findingRecord);
        //                         findingDataRecord = findingDataRepository.save(findingDataRecord);

        //                         packageRecord.getFindings().add(findingRecord);
        //                         packageRecord = packageRepository.save(packageRecord);

        //                         var updatedFindingReporters = new HashSet<FindingReporter>();
        //                         for (var findingReporterRecord : findingReporters) {
        //                             findingReporterRecord.getFindings().add(findingRecord);
        //                             var updatedRecord = findingReporterRepository.save(findingReporterRecord);   
        //                             updatedFindingReporters.add(updatedRecord);                                     
        //                         }

        //                         // now the inverse
        //                         findingRecord.getReporters().addAll(updatedFindingReporters);
        //                         findingRecord.getPackages().add(packageRecord);
        //                         findingRecord.setData(findingDataRecord);
        //                         findingRecord = findingRepository.save(findingRecord);

        //                     } 
        //                     // otherwise add to the existing record 
        //                     else {
        //                         var findingRecord = findingDataRecord.getFinding();
                                
        //                         if ( !findingRecord.getPackages().contains(packageRecord)) {
        //                             findingRecord.getPackages().add(packageRecord);
        //                             findingRecord = findingRepository.save(findingRecord);

        //                             packageRecord.getFindings().add(findingRecord);
        //                             packageRecord = packageRepository.save(packageRecord);
        //                         }

        //                         // 
        //                         for (var findingReporterRecord : findingReporters) {
        //                             if ( !findingRecord.getReporters().contains(findingReporterRecord)) {
        //                                 findingRecord.getReporters().add(findingReporterRecord);
        //                                 findingRecord = findingRepository.save(findingRecord);
    
        //                                 findingReporterRecord.getFindings().add(findingRecord);
        //                                 findingReporterRecord = findingReporterRepository.save(findingReporterRecord);                                        
        //                             }
        //                         }

        //                         // no need to add findingData - it's already there 
        //                     }

        //                 }
                            
        //             }

        //             // if we made it this far we need to update the status of the event object from INGESTING to 
        //             // READY_FOR_PROCESSING
        //             var datasourceEvents = datasourceEventRepository.findByTxid(response.getTxid());
                    
        //             // TODO
        //             // migrate all of these integrity checks to a single helper 
        //             if (datasourceEvents.size() > 1) {
        //                 log.error(
        //                     "something went wrong. table datasourceEvents should have only one record per txid {}. " +
        //                     "but had {}. system integrity compromised. throwing exception...",
        //                     response.getTxid(),
        //                     datasourceEvents.size()
        //                 );
        //                 throw new IllegalStateException();
        //             } else if (datasourceEvents.isEmpty()) {
        //                 log.warn(
        //                     "something went wrong. no datasourceEvent record exists for txid {}. " +
        //                     "there should never be a response to this service from this caller that did not result " +
        //                     "from a datasourceEvent having been processed. system integrity may be compromised...",
        //                     response.getTxid()
        //                 );                       
        //             } else {
        //                 var datasourceEventRecord = datasourceEvents.get(0);
        //                 datasourceEventRecord.setProcessingStatus(DatasourceEvent.Status.READY_FOR_PROCESSING);
        //                 datasourceEventRecord = datasourceEventRepository.save(datasourceEventRecord);

        //                 // now check on dataset and datasource status 
        //                 var datasource = datasourceEventRecord.getDatasource();

        //                 if (datasource.getStatus() != Datasource.Status.INITIALIZING) {
        //                     datasource.setStatus(Datasource.Status.READY_FOR_PROCESSING);
        //                     datasource = datasourceRepository.save(datasource);
        //                 }

        //                 // TODO see above
        //                 var datasets = datasource.getDatasets();
        //                 for (var dset : datasets) {
        //                     boolean okToSetStatus = true;
        //                     for (var dsrc : dset.getDatasources()) {
        //                         if (dsrc.getStatus().equals(Datasource.Status.INITIALIZING)) {
        //                             okToSetStatus = false;
        //                             break;
        //                         }
        //                     }
        //                     if (okToSetStatus) {
        //                         dset.setStatus(Dataset.Status.READY_FOR_PROCESSING);
        //                         datasetRepository.save(dset);
        //                     }
        //                 }

        //             }


                    
        //         }
        //         break;
        //     default:
        //         break;
        // }
    }


    //
    // helpers 
    //


    /**
     * 
     * @param topic
     * @param apiRequest
     */
    public void makeRequest(String topic, ApiRequest apiRequest) throws IllegalArgumentException {
        log.info("servicing apiRequest as Kafka message: {}", apiRequest);
        if ( !apiRequest.isValidForKafka() ) { 
            log.error("request obj failed validity check - rejecting and throwing exception");
            throw new IllegalArgumentException(); 
        }

        kafkaRequestTemplate.send(topic, apiRequest);
    }


    /**
     * helper to invoke the handler method we already know is associated with a given REST URI. The method allows us 
     * to invoke the method with the correct arguments w/o having to deal with a lot of wonky reflection that would 
     * otherwise be necessary. 
     * 
     * @param txid
     * @param verb
     * @param resource
     * @param handlerMethod
     * @param requestReceivedAt
     * @return
     * @throws InvocationTargetException 
     * @throws IllegalAccessException 
     */
    private ApiResponse invokeMethod(
            UUID txid,
            String verb,
            String resource,
            HandlerMethod handlerMethod, 
            ZonedDateTime requestReceivedAt,
            ApiRequest apiRequest
    ) throws IllegalAccessException, InvocationTargetException  {
        // this should never happen so long as we call the RestInfoService helper methods first to 
        // get a hold of the reflected Method obj representing the controller for the requested resource.
        // I don't like returning null so we assume we can't find what we're looking for until we find it
        // and replace this with whatever is more appropriate. 
        var rv = ApiResponse.builder()
                            .responderName(env.getServiceName())
                            .code(Response.SC_NOT_FOUND)
                            .txid(txid)
                            .requestReceivedAt(requestReceivedAt.toString())
                            .build();
                            
        var restSignature = verb + "_" + resource;

        var bean = handlerMethod.getBean();
        // I think this is a name only until the object is actually created. It's typed as an "Object" in the 
        // HandlerMethod class.
        if (bean.getClass() == String.class) { bean = context.getBean((String)bean); }
        var beanMethod = handlerMethod.getMethod();
        log.debug("bean method name is: {}", beanMethod.getName());
        log.debug("bean var class is: {}", bean.getClass());
        log.debug("bean type is: {}", handlerMethod.getBeanType());
        log.debug("beanMethod is: {}", beanMethod);

        /*
         * 
         * WHEN YOU ADD A NEW REST CONTROLLER/SERVICE THIS IS WHERE YOU ADD THE HOOK TO ENSURE THE KAFKA LISTENER
         * KNOWS HOW TO INVOKE THE CONTROLLER METHOD
         * 
         */
        switch(restSignature) {
            case HealthCheckController.GET_PING_SIGNATURE:
            case RestInfoController.GET_REST_INFO_SIGNATURE:
                var re = (ResponseEntity<ApiResponse>)beanMethod.invoke(bean, txid, requestReceivedAt);
                rv = re.getBody();
                break;
            case InputController.POST_INPUT_GIT_SIGNATURE:
                var datasourceEvent = apiRequest.getData().get("datasourceEvent");
                var eventFileData = apiRequest.getData().get("eventFileData");
                re = (ResponseEntity<ApiResponse>)beanMethod.invoke(bean, txid, requestReceivedAt, datasourceEvent, eventFileData);
                rv = re.getBody();
                break;
        }
        return rv;
    }

}
