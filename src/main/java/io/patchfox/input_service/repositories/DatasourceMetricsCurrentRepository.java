package io.patchfox.input_service.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import io.patchfox.db_entities.entities.DatasourceMetricsCurrent;

public interface DatasourceMetricsCurrentRepository extends JpaRepository<DatasourceMetricsCurrent, Long> {}
