-- PROCEDURE: Data.usp_CashFlow()

-- DROP PROCEDURE IF EXISTS "Data"."usp_CashFlow"();

CREATE OR REPLACE PROCEDURE "Data"."usp_CashFlow"(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
    total_rows_insert integer;
    total_rows_update integer;
    total_rows_extract integer;
    v_sync_id BIGINT;
BEGIN
    -- Drop temporary table if it exists
    DROP TABLE IF EXISTS Temp_CashFlow;
    
    -- Create temporary table using a CTE to filter duplicates
    CREATE TEMP TABLE Temp_CashFlow 
    (
        "CashFlowID"      INT,
        "AccountID"       INT,
        "TransactionDate" TIMESTAMP,  
        "CashInflow"      DECIMAL(16,2),
        "CashOutflow"     DECIMAL(16,2),
        "Description"     VARCHAR(250)
    );
    
    BEGIN
         SELECT COUNT(*) 
           INTO total_rows_extract 
         FROM "stage"."CashFlow";
         RAISE NOTICE 'Extracted % rows', total_rows_extract;
    END;
IF total_rows_extract>0 THEN    
    WITH CTE_CashFlow AS (
         SELECT 
             ROW_NUMBER() OVER (
                 PARTITION BY "CashFlowID", "AccountID" 
                 ORDER BY "CashFlowID", "AccountID"
             ) AS rn,
             CAST("CashFlowID" AS INT) AS "CashFlowID",
             CAST("AccountID" AS INT) AS "AccountID",
             CAST("TransactionDate" AS TIMESTAMP) AS "TransactionDate",
             CAST("CashInflow" AS DECIMAL(16,2)) AS "CashInflow",
             CAST("CashOutflow" AS DECIMAL(16,2)) AS "CashOutflow",
             CAST("Description" AS VARCHAR(250)) AS "Description"
         FROM "stage"."CashFlow" 
    )
    INSERT INTO Temp_CashFlow (
        "CashFlowID",    
        "AccountID",    
        "TransactionDate", 
        "CashInflow",    
        "CashOutflow",    
        "Description" 
    )
    SELECT 
         "CashFlowID",  
         "AccountID",  
         "TransactionDate", 
         "CashInflow",  
         "CashOutflow",  
         "Description"
    FROM CTE_CashFlow 
    WHERE rn = 1;
    
    -- Insert rows from Temp_CashFlow that do not exist in Data.CashFlow
    INSERT INTO "Data"."CashFlow" (
         "CashFlowID", 
         "AccountID", 
         "TransactionDate", 
         "CashInflow", 
         "CashOutflow", 
         "Description"
    )
    SELECT 
         CAST(T."CashFlowID" AS INT),
         CAST(T."AccountID" AS INT),
         CAST(T."TransactionDate" AS TIMESTAMP),
         CAST(T."CashInflow" AS DECIMAL(16,2)),
         CAST(T."CashOutflow" AS DECIMAL(16,2)),
         CAST(T."Description" AS VARCHAR(250))
    FROM Temp_CashFlow T
    LEFT JOIN "Data"."CashFlow" BCF 
      ON BCF."CashFlowID" = CAST(T."CashFlowID" AS INT)
    WHERE BCF."CashFlowID" IS NULL;
    
    BEGIN
         GET DIAGNOSTICS total_rows_insert = ROW_COUNT;
         RAISE NOTICE 'Inserted % rows', total_rows_insert;
    END;
    
    -- Update existing rows in Data.CashFlow with data from stage.CashFlow
    UPDATE "Data"."CashFlow" BCF
    SET 
         "AccountID"         = CAST(SCF."AccountID" AS INT),
         "TransactionDate"   = CAST(SCF."TransactionDate" AS TIMESTAMP),
         "CashInflow"        = CAST(SCF."CashInflow" AS DECIMAL(16,2)),
         "CashOutflow"       = CAST(SCF."CashOutflow" AS DECIMAL(16,2)),
         "Description"       = CAST(SCF."Description" AS VARCHAR(250)),
         "RecordModifiedDate"= CURRENT_TIMESTAMP,
         "RecordModifiedBy"  = CURRENT_USER
    FROM "stage"."CashFlow" SCF
    WHERE BCF."CashFlowID" = CAST(SCF."CashFlowID" AS INT);
    
    BEGIN
         GET DIAGNOSTICS total_rows_update = ROW_COUNT;
         RAISE NOTICE 'Updated % rows', total_rows_update;
    END;
 END IF;   
   -- Extract the sync_id from the _airbyte_meta column in stage.CashFlow
    SELECT "ExecutionLogId" INTO v_sync_id FROM "Audit"."ExecutionLog" WHERE "JobID" IN (
	SELECT (_airbyte_meta::json->>'sync_id')::BIGINT
    FROM "stage"."CashFlow"
    LIMIT 1);
    
    -- Insert summary information into TableProcessing, including the ExecutionLogID
    INSERT INTO "Audit"."TableProcessing"
         ("TableName", "ExtractRowCnt", "InsertRowCnt", "UpdateRowCnt", "ExecutionLogID")
    VALUES 
         (
             'Data.CashFlow',
             total_rows_extract,
             total_rows_insert,
             total_rows_update,
             v_sync_id
         );    
END;
$BODY$;
ALTER PROCEDURE "Data"."usp_CashFlow"()
    OWNER TO postgres;
