CREATE OR REPLACE FUNCTION create_and_fetch_or_fetch_dataset (
 in_name varchar,
 in_time timestamptz,
 in_uuid uuid
)
RETURNS TABLE (
 id bigint,
 latest_job_id uuid,
 latest_txid uuid,
 name varchar,
 status varchar,
 updated_at timestamptz
) AS '
    BEGIN
    IF NOT EXISTS (SELECT 1 FROM dataset WHERE dataset.name = in_name) THEN
        INSERT INTO dataset (name, updated_at, latest_txid, status)
        VALUES (in_name, in_time, in_uuid, ''INITIALIZING'');
    END IF;
    
    UPDATE dataset
        SET 
            updated_at = in_time, 
            latest_txid = in_uuid
        WHERE dataset.name = in_name;
    
    RETURN QUERY 
        SELECT 
            d.id, 
            d.latest_job_id, 
            d.latest_txid, 
            d.name, 
            d.status, 
            d.updated_at 
        FROM dataset d 
        WHERE d.name = in_name;
    END;
' LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION create_and_fetch_or_fetch_datasource (
    in_dataset_ids_str_encoded_array varchar,
    in_commit_branch varchar,
    in_domain varchar,
    in_event_received_at timestamptz,
    in_txid uuid,
    in_datasource_packed_name varchar,
    in_datasource_purl varchar,
    in_datasource_type varchar,
    in_array_delimiter varchar
) RETURNS TABLE (
    id bigint,
    commit_branch varchar,
    domain varchar,
    first_event_received_at timestamptz,
    last_event_received_at timestamptz,
    last_event_received_status varchar,
    latest_job_id uuid,
    latest_txid uuid,
    name varchar,
    number_event_processing_errors double precision, 
    number_events_received double precision, 
    package_indexes bigint[],
    purl varchar,
    status varchar,
    type varchar
) AS '

    DECLARE
        dataset_pk bigint;
        datasource_pk bigint;
        tmp_number_events_received int;
        in_dataset_ids bigint[];

    BEGIN
		-- convert string_array args to array type
		-- this is because there is no array type in standard SQL and hibernate is a butt about it 
		-- also spring-data/hibernate is a butt about $$ delimeters so we are using single quote but that messes with
		-- the invocation of string_to_array which uses single quotes to define the delimiter for the array - hence 
		-- we are using a caller supplied argument. Normally '' will work but it does not seem to work in the context
		-- of invocation of a postgres function. 
		SELECT string_to_array(in_dataset_ids_str_encoded_array::text, in_array_delimiter::text) INTO in_dataset_ids;

        IF NOT EXISTS (SELECT 1 FROM datasource WHERE datasource.purl = in_datasource_purl) THEN
            INSERT INTO datasource (
                commit_branch,
                domain,
                first_event_received_at,
                last_event_received_at,
                last_event_received_status,
                latest_txid,
                name,
                number_event_processing_errors,
                number_events_received,
                purl,
                status,
                type
            )
            VALUES (
                in_commit_branch,
                in_domain,
                in_event_received_at,
                in_event_received_at,
                ''ACCEPTED'',
                in_txid,
                in_datasource_packed_name,
                0,
                1,
                in_datasource_purl,
                ''INGESTING'',
                in_datasource_type
            );
        END IF;
        
        -- create association between datasource and dataset records 
        SELECT d.id 
            FROM datasource d
            INTO datasource_pk
            WHERE d.purl = in_datasource_purl;

        FOR index IN 1..array_length(in_dataset_ids, 1) LOOP
            INSERT 
                INTO datasource_dataset (datasource_id, dataset_id)
                VALUES (datasource_pk, in_dataset_ids[index])
                ON CONFLICT DO NOTHING;
        END LOOP;

        -- grab current number_events_received value
        SELECT d.number_events_received
            FROM datasource d
            INTO tmp_number_events_received
            WHERE d.purl = in_datasource_purl;        

        UPDATE datasource
            SET 
                last_event_received_at = in_event_received_at,
                last_event_received_status = ''ACCEPTED'',
                latest_txid = in_txid,
                number_events_received = tmp_number_events_received + 1
            WHERE datasource.purl = in_datasource_purl;
        
        RETURN QUERY 
            SELECT 
                ds."id",
                ds.commit_branch,
                ds.domain,
                ds.first_event_received_at,
                ds.last_event_received_at,
                ds.last_event_received_status,
                ds.latest_job_id,
                ds.latest_txid,
                ds.name,
                ds.number_event_processing_errors,
                ds.number_events_received,
                ds.package_indexes,
                ds.purl,
                ds.status,
                ds.type 
            FROM datasource ds 
            WHERE ds.purl = in_datasource_purl;

    END;
' LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION create_and_associate_packages (
    in_datasource_event_id bigint,
    in_purl varchar,
    in_type varchar,
    in_namespace varchar,
    in_name varchar,
    in_version varchar,
    in_updated_at timestamptz
)
RETURNS bigint AS '
    DECLARE
        package_pk bigint;
    
    BEGIN
        -- Create package if it doesn''t exist
        IF NOT EXISTS (SELECT 1 FROM package WHERE package.purl = in_purl) THEN
            INSERT INTO package (
                purl,
                type,
                namespace,
                name,
                version,
                updated_at,
                number_versions_behind_head,
                number_major_versions_behind_head,
                number_minor_versions_behind_head,
                number_patch_versions_behind_head
            )
            VALUES (
                in_purl,
                in_type,
                in_namespace,
                in_name,
                in_version,
                in_updated_at,
                -1,
                -1,
                -1,
                -1
            );
        END IF;
        
        -- Get the package id
        SELECT p.id 
            FROM package p
            INTO package_pk
            WHERE p.purl = in_purl;
            
        -- Create association between package and datasource_event
        INSERT INTO datasource_event_package (datasource_event_id, package_id)
            VALUES (in_datasource_event_id, package_pk)
            ON CONFLICT DO NOTHING;
        
        -- Return just the package id
        RETURN package_pk;
    END;
' LANGUAGE PLPGSQL;
