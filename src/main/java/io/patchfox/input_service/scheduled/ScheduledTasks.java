package io.patchfox.input_service.scheduled;


import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

import org.hibernate.Hibernate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.Datasource;
import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.input_service.repositories.DatasetRepository;
import io.patchfox.input_service.repositories.DatasourceEventRepository;
import io.patchfox.input_service.repositories.DatasourceRepository;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
public class ScheduledTasks {

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    DatasourceRepository datasourceRepository;

    @Autowired
    DatasourceEventRepository datasourceEventRepository;

    /**
     * every minute check to see if we can toggle the INITIALIZING status on datasources and their dataset containers
     */
    @Transactional
    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.MINUTES)
    public void curateEventStatus() {
        checkDoneInitializing();
        checkDoneIngesting();
        checkIdle();
        //checkDatasetError();
    }


    /**
     * 
     */
    public void checkDoneInitializing() {

        // grab anything with a status of INITIALIZING that hasn't been updated in at least two minutes 
        log.info("begin checkDoneInitializing");

        // start with the datasources
        List<Datasource> datasources = datasourceRepository.findAllByStatusAndLastEventReceivedAtBefore(
            Datasource.Status.INITIALIZING,
            ZonedDateTime.now().minusMinutes(2)
        );     
        
        for (var datasource : datasources) {

            var gtg = true;
            List<DatasourceEvent> datasourceEvents = datasourceEventRepository.findAllByDatasourcePurl(datasource.getPurl());
            for (var datasourceEvent : datasourceEvents) {
                if (
                    datasourceEvent.getStatus() != DatasourceEvent.Status.READY_FOR_PROCESSING
                    && datasourceEvent.getStatus() != DatasourceEvent.Status.PROCESSED
                ) {
                    gtg = false;
                    break;
                }
            }

            if (gtg) {
                datasource.setStatus(Datasource.Status.READY_FOR_PROCESSING);   
                log.info("marking datasource {} READY_FOR_PROCESSING", datasource.getPurl());
                datasource = datasourceRepository.save(datasource);                
            }

        }

        // now check on the datasets
        List<Dataset> datasets = datasetRepository.findAllByStatus(Dataset.Status.INITIALIZING);
        for (var dataset : datasets) {
            var gtg = true;

            for (var datasource : dataset.getDatasources()) {
                if (
                    datasource.getStatus() == Datasource.Status.INITIALIZING 
                    || datasource.getStatus() == Datasource.Status.INGESTING
                ) {
                    gtg = false;
                    break;
                }
            }

            if (gtg) {
                dataset.setStatus(Dataset.Status.READY_FOR_PROCESSING);   
                log.info("marking dataset {} READY_FOR_PROCESSING", dataset.getName());
                dataset = datasetRepository.save(dataset);                
            }

        }

        // grab anything with a status of INITIALIZING that hasn't been updated in at least two minutes 
        log.info("done checkDoneInitializing");
    }


    /**
     * 
     * in case something went wrong during upload the INGESTING flag may have gotten stuck
     * 
     */
    public void checkDoneIngesting() {
        // grab anything with a status of INGESTING that hasn't been updated in at least two minutes 
        log.info("begin checkDoneIngesting");

        // start with the datasources
        List<Datasource> datasources = datasourceRepository.findAllByStatusAndLastEventReceivedAtBefore(
            Datasource.Status.INGESTING,
            ZonedDateTime.now().minusMinutes(2)
        );     
        
        if (datasources.isEmpty()) {
            log.info("no datasources found with status INGESTING older than 2m.");
        }

        for (var datasource : datasources) {
            //log.info("datasource is: {}", datasource);
            var hasReadyForProcessingEvent = false;
            var hasIngestingEvent = false;
            List<DatasourceEvent> datasourceEventsReadyForProcessing = 
                datasourceEventRepository.findAllByStatusAndDatasourcePurl(DatasourceEvent.Status.READY_FOR_PROCESSING, datasource.getPurl());
            //log.info("datasourceEvent size is: {}", datasourceEvents.size());
            
            List<DatasourceEvent> datasourceEventsIngesting = 
                datasourceEventRepository.findAllByStatusAndDatasourcePurl(DatasourceEvent.Status.INGESTING, datasource.getPurl());
            
            if ( !datasourceEventsReadyForProcessing.isEmpty() ) { hasReadyForProcessingEvent = true; }
            if ( !datasourceEventsIngesting.isEmpty()) { hasIngestingEvent = true; }
            
            log.info("hasReadyForProcessingEvent is: {}  hasIngestingEvent is: {}", hasReadyForProcessingEvent, hasIngestingEvent);
            var gtg = (hasReadyForProcessingEvent && !hasIngestingEvent);
            if (gtg) {
                log.info(
                    "READY_FOR_PROCESSING event(s) found for datasource: {} - marking datasource as READY_FOR_PROCESSING", 
                    datasource.getPurl()
                );                
                datasource.setStatus(Datasource.Status.READY_FOR_PROCESSING);   
            } else {
                log.info(
                    "no READY_FOR_PROCESSING or INGESTING events found for datasource: {} - marking datasource as IDLE", 
                    datasource.getPurl()
                );                
                datasource.setStatus(Datasource.Status.IDLE);   
            } 
            datasource = datasourceRepository.save(datasource);                
        }

        // now check on the datasets
        List<Dataset> datasets = datasetRepository.findAllByStatus(Dataset.Status.INGESTING);
        if (datasources.isEmpty()) {
            log.info("no datasets found with status INGESTING");
        }

        for (var dataset : datasets) {
            var gtg = false;

            for (var datasource : dataset.getDatasources()) {
                if (datasource.getStatus() == Datasource.Status.READY_FOR_PROCESSING) {
                    gtg = true;
                    break;
                }
            }

            if (gtg) {
                log.info(
                    "found datasource with READY_FOR_PROCESSING data for dataset {} - marking dataset as READY_FOR_PROCESSING", 
                    dataset.getName()
                );
                dataset.setStatus(Dataset.Status.READY_FOR_PROCESSING); 
            } else {
                log.info(
                    "no datasources found in state INGESTING for dataset {} - marking dataset as IDLE", 
                    dataset.getName()
                );
                dataset.setStatus(Dataset.Status.IDLE);   
            } 

            dataset = datasetRepository.save(dataset);                
        }

        // grab anything with a status of INITIALIZING that hasn't been updated in at least two minutes 
        log.info("done checkDoneIngesting");        
    }


    /**
     * 
     */
    public void checkIdle() {
        log.info("begin checkIdle");

        //
        // in case a dataset got left dangling in a PROCESSING state but is in fact idle 
        //
        List<Dataset> processingDatasets = datasetRepository.findAllByStatus(Dataset.Status.PROCESSING);
        
        for (var dataset : processingDatasets) {

            boolean doneProcessing = true; 
            for (var datasource : dataset.getDatasources()) {
                if (
                    datasource.getStatus().equals(Datasource.Status.PROCESSING)
                    || datasource.getStatus().equals(Datasource.Status.INGESTING)
                    || datasource.getStatus().equals(Datasource.Status.INITIALIZING)   
                    || datasource.getStatus().equals(Datasource.Status.READY_FOR_NEXT_PROCESSING)   
                ) {
                    doneProcessing = false;
                    break;
                }

            }

            if (doneProcessing) {
                dataset.setStatus(Dataset.Status.IDLE);
                dataset = datasetRepository.save(dataset);
            }
        }

        List<Dataset> idleDatasets = datasetRepository.findAllByStatus(Dataset.Status.IDLE);
        var datasetsToProcess = new ArrayList<Dataset>();
        for (var dataset : idleDatasets) {
            for (var datasource : dataset.getDatasources()) {
                var datasourceReady = datasource.getStatus().equals(Datasource.Status.READY_FOR_PROCESSING);
                List<DatasourceEvent> datasourceEvents = 
                    datasourceEventRepository.findAllByStatusAndDatasourcePurl(DatasourceEvent.Status.READY_FOR_PROCESSING, datasource.getPurl());
                
                var datasourceEventReady = !datasourceEvents.isEmpty();
                if (datasourceReady || datasourceEventReady) {
                    log.info(
                        "discovered READY_FOR_PROCESSING datasource or datasourceEvent: {} in dataset: {}",
                        datasource.getPurl(),
                        dataset.getName()
                    );
                    datasetsToProcess.add(dataset);
                    break;
                }
            }
        }

        for (var dataset : datasetsToProcess) {
            log.info("marking dataset {} READY_FOR_PROCESSING", dataset.getName());
            dataset.setStatus(Dataset.Status.READY_FOR_PROCESSING);
            dataset.setUpdatedAt(ZonedDateTime.now());
            dataset = datasetRepository.save(dataset);

            for (var datasource : dataset.getDatasources()) {
                log.info("marking datasource {} READY_FOR_PROCESSING", datasource.getPurl());
                datasource.setStatus(Datasource.Status.READY_FOR_PROCESSING);
                datasource = datasourceRepository.save(datasource);
            }
        }

        log.info("done checkIdle");
    }



    /**
     * 
     */
    public void checkDatasetError() {
        log.info("start checkDatasetError");
        var now = ZonedDateTime.now();
        var sixHoursAgo = now.minusHours(6);
        var datasetsInProcessing = datasetRepository.findAllByStatusAndUpdatedAtBefore(Dataset.Status.PROCESSING, sixHoursAgo);
        for (var datasetInProcessing : datasetsInProcessing) {
            log.warn(
                "dataset: {} has been processing since: {}. checking datasources for error state...",
                datasetInProcessing.getName(),
                datasetInProcessing.getUpdatedAt()
            );
            for (var datasource : datasetInProcessing.getDatasources()) {
                if (datasource.getStatus().equals(Datasource.Status.PROCESSING_ERROR)) {
                    log.warn(
                        "datasource: {} has status PROCESSING_ERROR. setting error state on dataset: {}",
                        datasource.getPurl(),
                        datasetInProcessing.getName()
                    );

                    datasetInProcessing.setStatus(Dataset.Status.PROCESSING_ERROR);
                    datasetInProcessing = datasetRepository.save(datasetInProcessing);
                }
            }
        }

        log.info("done checkDatasetError");
    }


}
