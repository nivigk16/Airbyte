-- PROCEDURE: Data.usp_ChartOfAccounts()

-- DROP PROCEDURE IF EXISTS "Data"."usp_ChartOfAccounts"();

CREATE OR REPLACE PROCEDURE "Data"."usp_ChartOfAccounts"(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
    total_rows_insert integer;
    total_rows_update integer;
    total_rows_extract integer;
	v_sync_id BIGINT;

BEGIN
    -- Drop temporary table if it exists (NOTICE is expected if it doesn't exist)
    DROP TABLE IF EXISTS Temp_ChartOfAccounts;
    
    -- Create temporary table to store deduplicated data
    CREATE TEMP TABLE Temp_ChartOfAccounts (
         "AccountID"        INT,
         "AccountName"      VARCHAR(100),
         "AccountType"      VARCHAR(50),
         "AccountSubType"   VARCHAR(50),
         "ParentAccountID"  INT,
		 "EntityID" INT
    );
BEGIN
     SELECT COUNT(*) into total_rows_extract FROM "stage"."ChartOfAccounts";
    	RAISE NOTICE 'Extracted % rows', total_rows_extract;
END;	
 IF total_rows_extract>0 THEN   
    -- Use a CTE to deduplicate rows based on AccountID
    WITH CTE_ChartOfAccounts AS (
         SELECT 
             ROW_NUMBER() OVER (
                 PARTITION BY "AccountID" 
                 ORDER BY "AccountID"
             ) AS rn,
             CAST("AccountID" AS INT) AS "AccountID",
             CAST("AccountName" AS VARCHAR(100)) AS "AccountName",
             CAST("AccountType" AS VARCHAR(50)) AS "AccountType",
             CAST("SubType" AS VARCHAR(50)) AS "AccountSubType",
             CASE WHEN "ParentAccountID"!= '' THEN CAST("ParentAccountID" AS INT) ELSE NULL END AS "ParentAccountID",
			 CASE WHEN "EntityID"!= '' THEN CAST("EntityID" AS INT) ELSE NULL END AS "EntityID"
         FROM "stage"."ChartOfAccounts"
    )
    INSERT INTO Temp_ChartOfAccounts (
         "AccountID",
         "AccountName",
         "AccountType",
         "AccountSubType",
         "ParentAccountID",
		 "EntityID"
    )
    SELECT
         CAST("AccountID" AS INT) AS "AccountID",
             CAST("AccountName" AS VARCHAR(100)) AS "AccountName",
             CAST("AccountType" AS VARCHAR(50)) AS "AccountType",
             CAST("AccountSubType" AS VARCHAR(50)) AS "AccountSubType",
             CAST("ParentAccountID" AS INT) AS "ParentAccountID",
			 CAST("EntityID" AS INT) AS "EntityID"
    FROM CTE_ChartOfAccounts
    WHERE rn = 1;

    -- Insert rows from Temp_ChartOfAccounts that do not exist in Data.ChartOfAccounts
    INSERT INTO "Data"."ChartOfAccounts" (
         "AccountID",
         "AccountName",
         "AccountType",
         "AccountSubType",
         "ParentAccountID",
		 "EntityID"
    )
    SELECT 
         T."AccountID",
         T."AccountName",
         T."AccountType",
         T."AccountSubType",
         T."ParentAccountID",
		 T."EntityID"
    FROM Temp_ChartOfAccounts T
    LEFT JOIN "Data"."ChartOfAccounts" BCA
      ON BCA."AccountID" = T."AccountID"
    WHERE BCA."AccountID" IS NULL;
BEGIN
    GET DIAGNOSTICS total_rows_insert = ROW_COUNT;
    RAISE NOTICE 'Inserted % rows', total_rows_insert;
END;	
    
    -- Update existing rows in Data.ChartOfAccounts with data from stage.ChartOfAccounts
    UPDATE "Data"."ChartOfAccounts" BCA
    SET 
         "AccountName"      = CAST(SCA."AccountName" AS VARCHAR(100)),
         "AccountType"      = CAST(SCA."AccountType" AS VARCHAR(50)),
         "AccountSubType"   = CAST(SCA."SubType" AS VARCHAR(50)),
         "ParentAccountID"  = CASE WHEN SCA."ParentAccountID"!= '' THEN CAST(SCA."ParentAccountID" AS INT) ELSE NULL END,
		 "EntityID"  = CASE WHEN SCA."EntityID"!= '' THEN CAST(SCA."EntityID" AS INT) ELSE NULL END,
         "RecordModifiedDate" = CURRENT_TIMESTAMP,
         "RecordModifiedBy"   = CURRENT_USER
    FROM "stage"."ChartOfAccounts" SCA
    WHERE BCA."AccountID" = CAST(SCA."AccountID" AS INT);
BEGIN
    GET DIAGNOSTICS total_rows_update = ROW_COUNT;
    RAISE NOTICE 'Updated % rows', total_rows_update;
END;
END IF;
    -- Extract the sync_id from the _airbyte_meta column in stage.CashFlow
    SELECT "ExecutionLogId" INTO v_sync_id FROM "Audit"."ExecutionLog" WHERE "JobID" IN (
	SELECT (_airbyte_meta::json->>'sync_id')::BIGINT
    FROM "stage"."ChartOfAccounts"
    LIMIT 1);
    
    -- Insert summary information into TableProcessing, including the ExecutionLogID
    INSERT INTO "Audit"."TableProcessing" 
         ("TableName", "ExtractRowCnt", "InsertRowCnt", "UpdateRowCnt", "ExecutionLogID")
    VALUES 
         (
             'Data.ChartOfAccounts',
             total_rows_extract,
             total_rows_insert,
             total_rows_update,
             v_sync_id
         );
END;
$BODY$;
ALTER PROCEDURE "Data"."usp_ChartOfAccounts"()
    OWNER TO postgres;
