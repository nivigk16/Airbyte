-- PROCEDURE: Data.usp_EquityChanges()

-- DROP PROCEDURE IF EXISTS "Data"."usp_EquityChanges"();

CREATE OR REPLACE PROCEDURE "Data"."usp_EquityChanges"(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
    total_rows_insert integer;
    total_rows_update integer;
    total_rows_extract integer;
	v_sync_id BIGINT;
	
BEGIN
DROP TABLE IF EXISTS Temp_EquityChanges;
    
    -- Create temporary table using a CTE to filter duplicates
    CREATE TEMP TABLE Temp_EquityChanges 
	(
	"EquityChangeID" INT,
    "AccountID" INT,
    "ChangeDate" TIMESTAMP,
    "Amount" DOUBLE PRECISION,
    "ChangeType" VARCHAR(100),
    "Description" VARCHAR(250)
	);
BEGIN
     SELECT COUNT(*) into total_rows_extract FROM "stage"."CashFlow";
    	RAISE NOTICE 'Extracted % rows', total_rows_extract;
END;
IF total_rows_extract>0 THEN
    WITH CTE_EquityChanges AS (
         SELECT 
             ROW_NUMBER() OVER (
                 PARTITION BY "EquityChangeID" 
                 ORDER BY "EquityChangeID"
             ) AS rn,
			 CAST("EquityChangeID" AS INT) AS "EquityChangeID",
             CAST("AccountID" AS INT) AS "AccountID",
             CAST("ChangeDate" AS TIMESTAMP) AS "ChangeDate",
             CAST("Amount" AS DOUBLE PRECISION) AS "Amount",
             CAST("ChangeType" AS VARCHAR(100)) AS "ChangeType",
             CAST("Description" AS VARCHAR(250)) AS "Description"
         FROM "stage"."EquityChanges" 
    )
	INSERT INTO Temp_EquityChanges(
	"EquityChangeID",
	"AccountID"	
	,"ChangeDate"	
	,"Amount" 
	,"ChangeType"	
	,"Description"	
	)
    SELECT "EquityChangeID"
	,"AccountID"	
	,"ChangeDate"	
	,"Amount" 
	,"ChangeType"	
	,"Description"
    FROM CTE_EquityChanges 
    WHERE rn = 1;
	
	INSERT INTO "Data"."EquityChanges" (
		"EquityChangeID"
        ,"AccountID"	
		,"ChangeDate"	
		,"Amount" 
		,"ChangeType"	
		,"Description"
    )
    SELECT 
	CAST(T."EquityChangeID" AS INT) AS "EquityChangeID",
     CAST(T."AccountID" AS INT) AS "AccountID",
             CAST(T."ChangeDate" AS TIMESTAMP) AS "ChangeDate",
             CAST(T."Amount" AS DOUBLE PRECISION) AS "Amount",
             CAST(T."ChangeType" AS VARCHAR(100)) AS "ChangeType",
             CAST(T."Description" AS VARCHAR(250)) AS "Description"
    FROM Temp_EquityChanges T
    LEFT JOIN "Data"."EquityChanges" BEC 
      ON BEC."ChangeDate" = CAST(T."ChangeDate" AS TIMESTAMP)
	  AND BEC."AccountID" = CAST(T."AccountID" AS INT)
	  AND BEC."ChangeType" =T."ChangeType"
    WHERE BEC."ChangeDate" IS NULL AND BEC."AccountID" IS NULL
	AND BEC."ChangeType" IS NULL;
BEGIN
    GET DIAGNOSTICS total_rows_insert = ROW_COUNT;
    RAISE NOTICE 'Inserted % rows', total_rows_insert;
END;	
	
	 UPDATE "Data"."EquityChanges" BEC
    SET 
         "AccountID"       = CAST(SEC."AccountID" AS INT),
         "ChangeDate" = CAST(SEC."ChangeDate" AS TIMESTAMP),
         "Amount"      = CAST(SEC."Amount" AS DOUBLE PRECISION),
		  "ChangeType" =CAST(SEC."ChangeType" AS VARCHAR(100)),
         "Description"     = CAST(SEC."Description" AS VARCHAR(250)),
		 "RecordModifiedDate"=CURRENT_TIMESTAMP,
		 "RecordModifiedBy"=CURRENT_USER
    FROM "stage"."EquityChanges" SEC
    WHERE BEC."ChangeDate" = CAST(SEC."ChangeDate" AS TIMESTAMP) 
	  AND BEC."AccountID" = CAST(SEC."AccountID" AS INT)
	  AND BEC."ChangeType" =SEC."ChangeType";
BEGIN
    GET DIAGNOSTICS total_rows_update = ROW_COUNT;
    RAISE NOTICE 'Updated % rows', total_rows_update;
END;
END IF;
    -- Extract the sync_id from the _airbyte_meta column in stage.CashFlow
    SELECT "ExecutionLogId" INTO v_sync_id FROM "Audit"."ExecutionLog" WHERE "JobID" IN (
	SELECT (_airbyte_meta::json->>'sync_id')::BIGINT
    FROM "stage"."EquityChanges"
    LIMIT 1);
    
    -- Insert summary information into TableProcessing, including the ExecutionLogID
    INSERT INTO "Audit"."TableProcessing" 
         ("TableName", "ExtractRowCnt", "InsertRowCnt", "UpdateRowCnt", "ExecutionLogID")
    VALUES 
         (
             'Data.EquityChanges',
             total_rows_extract,
             total_rows_insert,
             total_rows_update,
             v_sync_id
         );
END;
$BODY$;
ALTER PROCEDURE "Data"."usp_EquityChanges"()
    OWNER TO postgres;
