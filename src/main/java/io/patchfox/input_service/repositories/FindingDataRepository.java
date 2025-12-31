package io.patchfox.input_service.repositories;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import io.patchfox.db_entities.entities.FindingData;

public interface FindingDataRepository extends JpaRepository<FindingData, Long>{
    List<FindingData> findByIdentifier(String identifier);
}
