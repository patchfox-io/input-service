package io.patchfox.input_service.helpers;


import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import com.github.packageurl.MalformedPackageURLException;
import com.github.packageurl.PackageURL;

import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.Datasource;
import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.db_entities.entities.Finding;
import io.patchfox.db_entities.entities.FindingData;
import io.patchfox.db_entities.entities.FindingReporter;
import io.patchfox.db_entities.entities.Package;
import io.patchfox.input_service.repositories.DatasetRepository;
import io.patchfox.input_service.repositories.DatasourceEventRepository;
import io.patchfox.input_service.repositories.DatasourceRepository;
import io.patchfox.input_service.repositories.FindingDataRepository;
import io.patchfox.input_service.repositories.FindingReporterRepository;
import io.patchfox.input_service.repositories.PackageRepository;
import io.patchfox.package_utils.data.oss.OssSummary;
import io.patchfox.package_utils.data.sbom.SbomPackageData;
import io.patchfox.package_utils.data.sbom.syft.SyftSbomPackageData;
import io.patchfox.package_utils.json.ApiResponse;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
public class HibernateHelper {

    @Autowired
    private PackageRepository packageRepository;

    @Autowired
    private FindingReporterRepository reporterRepository;

    @Autowired
    private FindingDataRepository findingDataRepository;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired 
    private DatasourceRepository datasourceRepository;

    @Autowired
    private DatasourceEventRepository datasourceEventRepository;


    /**
     * 
     * @param sbom
     * @param updatedAt
     * @throws MalformedPackageURLException
     */
    public void savePackages(
        Long datasourceEventRecordId,
        SbomPackageData<SyftSbomPackageData> sbom, 
        ZonedDateTime updatedAt
    ) throws MalformedPackageURLException {

        for (var pkg : sbom.getDependencyTree().getAllChildren(true)) {
            packageRepository.createAndAssociatePackage(
                datasourceEventRecordId,
                pkg.getPurl().toString(),
                pkg.getPurl().getType(),
                pkg.getPurl().getNamespace(),
                pkg.getPurl().getName(),
                pkg.getPurl().getVersion(),
                updatedAt
            );
            
        }
        
    }



    /**
     * 
     * @param reporters
     * @param ossSummary
     * @param reportedAt
     */
    public void saveFindingData(Set<String> reporters, OssSummary ossSummary, ZonedDateTime reportedAt) {
        for (var reporter : reporters) {
            var reporterRecord = FindingReporter.builder()
                                                .name(reporter)
                                                .build();

            var findingData = FindingData.builder()
                                         .identifier(ossSummary.getVulnerabilityId())
                                         .severity(ossSummary.getSeverity().toString())
                                         .description(ossSummary.getDescription())
                                         .cpes(ossSummary.getCpes())
                                         .reportedAt(reportedAt)
                                         .build();

            var finding = Finding.builder();
                                 

        }
    }



    /**
     * 
     * @param lastEventReceivedStatus
     * @param txid
     * @param requestReceivedAt
     * @return
     */
    public ApiResponse recordErrorAndGetApiResponse(
        String lastEventReceivedStatus,
        UUID txid,
        ZonedDateTime requestReceivedAt
    ) {
        var datasourceEventRecords = datasourceEventRepository.findAllByTxid(txid);

        if (datasourceEventRecords.isEmpty()) {
            log.error(
                "something went very wrong. this method should only be called after an event has already been " +
                "serialized to db. "
            );
            throw new IllegalStateException();
        } else if (datasourceEventRecords.size() > 1) {
            log.error(
                "something went very wrong. there should only be one record in the DatasourceEvent table " +
                "but there were {} records returned for txid {}. System integrity may be compromised.",
                datasourceEventRecords.size(), 
                txid
            );
            throw new IllegalStateException();
        }

        var datasourceEvent = datasourceEventRecords.get(0);
        var datasourcePurl = "";
        try {
            var datasourcePurlObj = new PackageURL(datasourceEvent.getPurl());
            datasourcePurl = datasourcePurlObj.getCoordinates();
        } catch (MalformedPackageURLException e) {
            log.error("malformed purl detected {}", datasourceEvent.getPurl());
            throw new IllegalArgumentException();
        }
        var datasourceRecords = datasourceRepository.findAllByPurl(datasourcePurl);

        if (datasourceRecords.isEmpty()) {
            log.error(
                "something went very wrong. DatasourceEvents should always have a record in the Datasource table"
            );
            throw new IllegalStateException();
        } else if (datasourceRecords.size() > 1) {
            log.error(
                "something went very wrong. there should only be one associated record in the Datasource table " +
                "but there were {} records returned for the same Datasource purl. System integrity may be compromised.",
                datasourceRecords.size(), 
                txid
            );
            throw new IllegalStateException();
        }        

        return recordErrorAndGetApiResponse(
            datasourceRecords.get(0),
            lastEventReceivedStatus,
            txid,
            requestReceivedAt
        );
    }


    /**
     * 
     * @param datasourceEventRecord
     * @param lastEventReceivedStatus
     * @param txid
     * @param requestReceivedAt
     * @return
     */
    public ApiResponse recordErrorAndGetApiResponse(
        DatasourceEvent datasourceEventRecord,
        String lastEventReceivedStatus,
        UUID txid,
        ZonedDateTime requestReceivedAt
    ) {

        // in case it's already there
        List<DatasourceEvent> datasourceEvents = datasourceEventRepository.findAllByPurl(datasourceEventRecord.getPurl());
        
        if (datasourceEvents.size() > 1) {
            log.info(
                "something has gone very wrong. purl should be a unique key in the db. " +
                "throwing exception. system integrity uncertain."
            );
            throw new IllegalArgumentException();
        } else if (datasourceEvents.size() == 1) {
            datasourceEventRecord = datasourceEvents.get(0);
        } else {
            // if event is not present then store the one we made upstream and set status appropriately 
            datasourceEventRecord.setStatus(DatasourceEvent.Status.PROCESSING_ERROR);

            // in case event processing failed in a way that prevented payload obj from being populated 
            // we can't store the event due to a null constraint on that field 
            if ( !datasourceEventRecord.isPayloadNull() ) {
                datasourceEventRecord = datasourceEventRepository.save(datasourceEventRecord);
            }
        }
        
        // process datasource and dataset
        return recordErrorAndGetApiResponse(
            datasourceEventRecord.getDatasource(),
            lastEventReceivedStatus, 
            txid, 
            requestReceivedAt
        );
    }



    /**
     * 
     * @param datasourceRecord
     * @param lastEventReceivedStatus
     * @param txid
     * @param requestReceivedAt
     * @return
     */
    public ApiResponse recordErrorAndGetApiResponse(
        Datasource datasourceRecord, 
        String lastEventReceivedStatus,
        UUID txid,
        ZonedDateTime requestReceivedAt
    ) {

        // process datasource 
        var numberEventProcessingErrors = datasourceRecord.getNumberEventProcessingErrors() + 1;
        datasourceRecord.setNumberEventProcessingErrors(numberEventProcessingErrors);
        datasourceRecord.setLastEventReceivedStatus(lastEventReceivedStatus);

        if ( !datasourceRecord.getStatus().equals(Datasource.Status.PROCESSING)) {
            datasourceRecord.setStatus(Datasource.Status.PROCESSING_ERROR);
        }
        
        datasourceRepository.save(datasourceRecord);

        // process datasets
        // for (var datasetRecord : datasourceRecord.getDatasets()) {
        //     if (!datasetRecord.getStatus().equals(Dataset.Status.PROCESSING)) {
        //         datasetRecord.setStatus(Dataset.Status.PROCESSING_ERROR);
        //     }
            
        //     datasetRepository.save(datasetRecord);
        // }

        // return error response - needs to be object of this type vs exception in case of kafka caller
        return ApiResponse.builder()
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .code(HttpStatus.BAD_REQUEST.value())
                          .build();
    }


    /**
     * 
     * @param datasetName
     * @param requestReceivedAt
     * @return
     */
    public Dataset fetchOrMakeAndFetchDatasetRecord(
        String datasetName, 
        ZonedDateTime requestReceivedAt,
        UUID txid
    ) throws IllegalStateException {
        List<Dataset> datasetRecords = datasetRepository.findAllByName(datasetName);
        if (datasetRecords.isEmpty()) {
            var newDataset =  Dataset.builder()
                                     .name(datasetName)
                                     .updatedAt(requestReceivedAt)
                                     .datasources(new HashSet<Datasource>())
                                     .latestTxid(txid)
                                     //.datasourceEvents(new HashSet<DatasourceEvent>())
                                     .status(Dataset.Status.INITIALIZING)
                                     .build(); 

            return datasetRepository.save(newDataset);
        } else if (datasetRecords.size() > 1) {
            throw new IllegalStateException(
                "datasetRecords unique column constraint has somehow been violated. " +
                "name should be enforced as unique. " +
                "Datasource database table integrity is uncertain."
            );
        } else {
            var dataset = datasetRecords.get(0);
            dataset.setLatestTxid(txid);
            dataset.setUpdatedAt(requestReceivedAt);
            // don't override the INITIALIZING flag here
            if (
                dataset.getStatus() != Dataset.Status.INITIALIZING
                && dataset.getStatus() != Dataset.Status.PROCESSING
            ) {
                dataset.setStatus(Dataset.Status.INGESTING);
            }
            
            return datasetRepository.save(dataset);

        }

    }


    /**
     * 
     * @param datasetRecord
     * @param eventDatasourcePurl
     * @param namespace
     * @param datasourcePackedName
     * @param type
     * @param requestReceivedAt
     * @return
     * @throws IllegalStateException
     */
    public Datasource fetchOrMakeAndFetchDatasourceRecord(
        Dataset datasetRecord,
        String eventDatasourcePurl, 
        String domain,
        String datasourcePackedName,
        String datasourceType,
        String commitBranch,
        ZonedDateTime requestReceivedAt,
        UUID txid
    ) throws IllegalStateException {
        List<Datasource> datasourceRecords = datasourceRepository.findAllByPurl(eventDatasourcePurl);
        Datasource datasourceRecord;
        var acceptedHttpStatus = HttpStatus.ACCEPTED.getReasonPhrase();

        if (datasourceRecords.isEmpty()) {
            datasourceRecord = Datasource.builder()
                                         .datasets(new HashSet<Dataset>(Set.of(datasetRecord)))
                                         //.datasourceEvents(new HashSet<DatasourceEvent>())
                                         .purl(eventDatasourcePurl)
                                         .domain(domain)
                                         .name(datasourcePackedName)
                                         .commitBranch(commitBranch)
                                         .type(datasourceType)
                                         .numberEventsReceived(1)
                                         .firstEventReceivedAt(requestReceivedAt)
                                         .lastEventReceivedAt(requestReceivedAt)
                                         .lastEventReceivedStatus(acceptedHttpStatus)
                                         .status(Datasource.Status.INITIALIZING)
                                         .latestTxid(txid)
                                         .build();
        } else {
            datasourceRecord = datasourceRecords.get(0);
            datasourceRecord.setLastEventReceivedAt(requestReceivedAt);
            datasourceRecord.setLastEventReceivedStatus(acceptedHttpStatus);
            datasourceRecord.setLatestTxid(txid);
            // don't override the INITIALIZING flag here
            if (
                datasourceRecord.getStatus() != Datasource.Status.INITIALIZING 
                && datasourceRecord.getStatus() != Datasource.Status.PROCESSING
                && datasourceRecord.getStatus() != Datasource.Status.READY_FOR_NEXT_PROCESSING
            ) {
                datasourceRecord.setStatus(Datasource.Status.INGESTING);
            }
            

            var numberEventsReceived = datasourceRecord.getNumberEventsReceived() + 1;
            datasourceRecord.setNumberEventsReceived(numberEventsReceived);
        }
        // record status to db to ensure event gets noted before we go further 
        return datasourceRepository.save(datasourceRecord);
    }

}
