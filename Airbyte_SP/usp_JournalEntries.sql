-- PROCEDURE: Data.usp_JournalEntries()

-- DROP PROCEDURE IF EXISTS "Data"."usp_JournalEntries"();

CREATE OR REPLACE PROCEDURE "Data"."usp_JournalEntries"(
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
    DROP TABLE IF EXISTS Temp_JournalEntries;
    
    -- Create temporary table using a CTE to filter duplicates
    CREATE TEMP TABLE Temp_JournalEntries 
	(
	"JournalID"	INT,
	"EntryDate"	TIMESTAMP,
	"Description"	VARCHAR(250),
	"TotalDebit"	DECIMAL(16,2),
	"TotalCredit" DECIMAL(16,2)
	);
BEGIN
     SELECT COUNT(*) into total_rows_extract FROM "stage"."JournalEntries";
    	RAISE NOTICE 'Extracted % rows', total_rows_extract;
END;
IF total_rows_extract>0 THEN
    WITH CTE_JournalEntries AS (
         SELECT 
             ROW_NUMBER() OVER (
                 PARTITION BY "JournalID"
                 ORDER BY "JournalID"
             ) AS rn,
             CAST("JournalID" AS INT) AS "JournalID",
             CAST("EntryDate" AS TIMESTAMP) AS "EntryDate",
             CAST("Description" AS VARCHAR(250)) AS "Description",
             CAST("TotalDebit" AS DECIMAL(16,2)) AS "TotalDebit",
             CAST("TotalCredit" AS DECIMAL(16,2)) AS "TotalCredit"
         FROM "stage"."JournalEntries"
    )
	INSERT INTO Temp_JournalEntries(
	"JournalID"	
	,"EntryDate"	
	,"Description" 
	,"TotalDebit"	
	,"TotalCredit"
	)
    SELECT "JournalID"	
	,"EntryDate"	
	,"Description" 
	,"TotalDebit"	
	,"TotalCredit"
    FROM CTE_JournalEntries 
    WHERE rn = 1;
    
    -- Insert rows from Temp_JournalEntries that do not exist in Data.JournalEntries
    INSERT INTO "Data"."JournalEntries" (
         "JournalID"	
	,"EntryDate"	
	,"Description" 
	,"TotalDebit"	
	,"TotalCredit"
    )
    SELECT 
         CAST(T."JournalID" AS INT),
         CAST(T."EntryDate" AS TIMESTAMP),
         CAST(T."Description" AS VARCHAR(250)),
         CAST(T."TotalDebit" AS DECIMAL(16,2)),
         CAST(T."TotalCredit" AS DECIMAL(16,2))
    FROM Temp_JournalEntries T
    LEFT JOIN "Data"."JournalEntries" BJE 
      ON BJE."JournalID" = CAST(T."JournalID" AS INT)
    WHERE BJE."JournalID" IS NULL;
BEGIN
    GET DIAGNOSTICS total_rows_insert = ROW_COUNT;
    RAISE NOTICE 'Inserted % rows', total_rows_insert;
END;	
    
    -- Update existing rows in Data.JournalEntries with data from stage.JournalEntries
    UPDATE "Data"."JournalEntries" BJE
    SET 
         "JournalID"       = CAST(SJE."JournalID" AS INT),
         "EntryDate"     = CAST(SJE."EntryDate" AS TIMESTAMP),
         "Description"      = CAST(SJE."Description" AS VARCHAR(250)),
         "TotalDebit"     = CAST(SJE."TotalDebit" AS DECIMAL(16,2)),
         "TotalCredit"     = CAST(SJE."TotalCredit" AS DECIMAL(16,2)),
		 "RecordModifiedDate"=CURRENT_TIMESTAMP,
		 "RecordModifiedBy"=CURRENT_USER
    FROM "stage"."JournalEntries" SJE
    WHERE BJE."JournalID" = CAST(SJE."JournalID" AS INT);
BEGIN
    GET DIAGNOSTICS total_rows_update = ROW_COUNT;
    RAISE NOTICE 'Updated % rows', total_rows_update;
END;
END IF;
-- Extract the sync_id from the _airbyte_meta column in stage.CashFlow
    SELECT "ExecutionLogId" INTO v_sync_id FROM "Audit"."ExecutionLog" WHERE "JobID" IN (
	SELECT (_airbyte_meta::json->>'sync_id')::BIGINT
    FROM "stage"."JournalEntries"
    LIMIT 1);
    
 -- Insert summary information into TableProcessing, including the ExecutionLogID
    INSERT INTO "Audit"."TableProcessing" 
         ("TableName", "ExtractRowCnt", "InsertRowCnt", "UpdateRowCnt", "ExecutionLogID")
    VALUES 
         (
             'Data.JournalEntries',
             total_rows_extract,
             total_rows_insert,
             total_rows_update,
             v_sync_id
         ); 
		 
END;
$BODY$;
ALTER PROCEDURE "Data"."usp_JournalEntries"()
    OWNER TO postgres;
