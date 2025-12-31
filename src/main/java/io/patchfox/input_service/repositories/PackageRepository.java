package io.patchfox.input_service.repositories;


import java.time.ZonedDateTime;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import io.patchfox.db_entities.entities.Package;

public interface PackageRepository extends JpaRepository<Package, Long> {
    List<Package> findByPurl(String purl);

    @Query(
        value = "SELECT CREATE_AND_ASSOCIATE_PACKAGES(" +
                    ":datasource_event_id, " +
                    ":purl, " +
                    ":type, " +
                    ":namespace, " +
                    ":name, " +
                    ":version, " +
                    "CAST(:updated_at AS timestamptz)" +
                ");",
        nativeQuery = true
    )
    Long createAndAssociatePackage(
        @Param("datasource_event_id") Long datasourceEventId,
        @Param("purl") String purl,
        @Param("type") String type,
        @Param("namespace") String namespace,
        @Param("name") String name,
        @Param("version") String version,
        @Param("updated_at") ZonedDateTime updatedAt
    );
}
