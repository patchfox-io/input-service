package io.patchfox.input_service.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import io.patchfox.db_entities.entities.DatasourceMetrics;

public interface DatasourceMetricsRepository extends JpaRepository<DatasourceMetrics, Long> {}
