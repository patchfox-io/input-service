package io.patchfox.input_service.repositories;


import java.util.UUID;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import io.patchfox.db_entities.entities.DatasourceEvent;


public interface DatasourceEventRepository extends JpaRepository<DatasourceEvent, Long> { 
    List<DatasourceEvent> findAllByTxid(UUID txid);

    List<DatasourceEvent> findAllByPurl(String purl);

    List<DatasourceEvent> findAllByDatasourcePurl(String purl);

    List<DatasourceEvent> findAllByStatusAndDatasourcePurl(DatasourceEvent.Status status, String purl);
}