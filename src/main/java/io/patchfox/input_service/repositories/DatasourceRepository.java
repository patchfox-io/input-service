package io.patchfox.input_service.repositories;

import java.util.List;
import java.util.UUID;
import java.time.ZonedDateTime;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.Datasource;


public interface DatasourceRepository extends JpaRepository<Datasource, Long> {
    List<Datasource> findAllByPurl(String purl);

    List<Datasource> findAllByStatus(Datasource.Status status);

    List<Datasource> 
        findAllByStatusAndLastEventReceivedAtBefore(
            Datasource.Status status,
            ZonedDateTime before
    );

    @Query(
        value = "SELECT * FROM CREATE_AND_FETCH_OR_FETCH_DATASOURCE(" +
                    ":in_dataset_ids_as_str, " +
                    ":in_commit_branch, " +
                    ":in_domain, " + 
                    "CAST(:in_event_received_at AS timestamptz), " + 
                    ":in_txid, " +
                    ":in_datasource_packed_name, " + 
                    ":in_datasource_purl, " +
                    ":in_datasource_type, " + 
                    ":in_array_delimiter" +
                ");", 
        nativeQuery = true
    )
    Datasource createAndFetchOrFetchDatasource (
        @Param("in_dataset_ids_as_str") String datasetIdsAsStr,
        @Param("in_commit_branch") String comitBranch,
        @Param("in_domain") String domain,
        @Param("in_event_received_at") ZonedDateTime eventReceivedAt,
        @Param("in_txid") UUID txid,
        @Param("in_datasource_packed_name") String name,
        @Param("in_datasource_purl") String purl,
        @Param("in_datasource_type") String datasourceType,
        @Param("in_array_delimiter") String arrayDelimiter
    );
    
}
