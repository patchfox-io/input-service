package io.patchfox.input_service.repositories;


import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import io.patchfox.db_entities.entities.FindingReporter;


public interface FindingReporterRepository extends JpaRepository<FindingReporter, Long> {
    List<FindingReporter> findByName(String name);
}
