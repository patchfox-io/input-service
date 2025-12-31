package io.patchfox.input_service.repositories;

import java.util.List;
import java.util.UUID;
import java.time.ZonedDateTime;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import io.patchfox.db_entities.entities.Dataset;

public interface DatasetRepository extends JpaRepository<Dataset, Long> {
    
    public List<Dataset> findAllByName(String name);

    public List<Dataset> findAllByStatus(Dataset.Status status);

    public List<Dataset> findAllByStatusAndUpdatedAtBefore(Dataset.Status status, ZonedDateTime updatedAt);

    @Query(
        value = "SELECT * FROM CREATE_AND_FETCH_OR_FETCH_DATASET(" +
                    ":p_dataset_name, " +
                    "CAST(:request_received_at AS timestamptz), " +
                    ":txid" + 
                ");", 
        nativeQuery = true
    )
    Dataset createAndFetchOrFetchDataset (
        @Param("p_dataset_name") String pDatasetName,
		@Param("request_received_at") ZonedDateTime updatedAt, 
		@Param("txid") UUID txid
    );

}
