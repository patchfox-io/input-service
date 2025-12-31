package io.patchfox.input_service.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import io.patchfox.db_entities.entities.Finding;

public interface FindingRepository extends JpaRepository<Finding, Long> {}
