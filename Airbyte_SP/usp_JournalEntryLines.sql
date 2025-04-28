-- PROCEDURE: Data.usp_JournalEntryLines()

-- DROP PROCEDURE IF EXISTS "Data"."usp_JournalEntryLines"();

CREATE OR REPLACE PROCEDURE "Data"."usp_JournalEntryLines"(
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
    DROP TABLE IF EXISTS Temp_JournalEntryLines;
    
    -- Create temporary table using a CTE to filter duplicates
    CREATE TEMP TABLE Temp_JournalEntryLines
	(
	"JournalEntryLineID" INT,
	"JournalID"	INT,
	"AccountID"	INT,
	"DebitAmount"	DECIMAL(16,2),
	"CreditAmount" DECIMAL(16,2),
	"Description" VARCHAR(250)
	);
BEGIN
     SELECT COUNT(*) into total_rows_extract FROM "stage"."JournalEntryLines";
    	RAISE NOTICE 'Extracted % rows', total_rows_extract;
END;	
IF total_rows_extract>0 THEN	
    WITH CTE_JournalEntryLines AS (
         SELECT 
             ROW_NUMBER() OVER (
                 PARTITION BY "LineID"
                 ORDER BY "LineID"
             ) AS rn,
             CAST("LineID" AS INT) AS "JournalEntryLineID",
             CAST("JournalID" AS INT) AS "JournalID",
             CAST("AccountID" AS INT) AS "AccountID",
             CAST("Debit" AS DECIMAL(16,2)) AS "DebitAmount",
             CAST("Credit" AS DECIMAL(16,2)) AS "CreditAmount",
			 CAST("Description" AS VARCHAR(250)) AS "Description"
         FROM "stage"."JournalEntryLines"
    )

	INSERT INTO Temp_JournalEntryLines(
	
	"JournalEntryLineID"	
	,"JournalID"	
	,"AccountID" 
	,"DebitAmount"	
	,"CreditAmount"
	,"Description"
	)
    SELECT "JournalEntryLineID"	
	,"JournalID"	
	,"AccountID" 
	,"DebitAmount"	
	,"CreditAmount"
	,"Description"
    FROM CTE_JournalEntryLines 
    WHERE rn = 1;
    
    -- Insert rows from Temp_JournalEntryLines that do not exist in Data.JournalEntryLines
    INSERT INTO "Data"."JournalEntryLines" (
	"JournalEntryLineID"	
	,"JournalID"	
	,"AccountID" 
	,"DebitAmount"	
	,"CreditAmount"
	,"Description"
    )
    SELECT 
         CAST(T."JournalEntryLineID" AS INT),
         CAST(T."JournalID" AS INT),
         CAST(T."AccountID" AS INT),
         CAST(T."DebitAmount" AS DECIMAL(16,2)),
         CAST(T."CreditAmount" AS DECIMAL(16,2)),
		 CAST(T."Description" AS VARCHAR(250))
    FROM Temp_JournalEntryLines T
    LEFT JOIN "Data"."JournalEntryLines" BJE 
      ON BJE."JournalEntryLineID" = CAST(T."JournalEntryLineID" AS INT)
    WHERE BJE."JournalEntryLineID" IS NULL;
BEGIN
    GET DIAGNOSTICS total_rows_insert = ROW_COUNT;
    RAISE NOTICE 'Inserted % rows', total_rows_insert;
END;	
    
    -- Update existing rows in Data.JournalEntryLines with data from stage.JournalEntryLines
    UPDATE "Data"."JournalEntryLines" BJE
    SET 
         "JournalID"     = CAST(SJE."JournalID" AS INT),
         "AccountID"      = CAST(SJE."AccountID" AS INT),
         "DebitAmount"     = CAST(SJE."Debit" AS DECIMAL(16,2)),
         "CreditAmount"     = CAST(SJE."Credit" AS DECIMAL(16,2)),
		 "Description"     = CAST(SJE."Description" AS VARCHAR(250)),
		 "RecordModifiedDate"=CURRENT_TIMESTAMP,
		 "RecordModifiedBy"=CURRENT_USER
    FROM "stage"."JournalEntryLines" SJE
    WHERE BJE."JournalEntryLineID" = CAST(SJE."LineID" AS INT);
BEGIN
    GET DIAGNOSTICS total_rows_update = ROW_COUNT;
    RAISE NOTICE 'Updated % rows', total_rows_update;
END;
END IF;
-- Extract the sync_id from the _airbyte_meta column in stage.CashFlow
    SELECT "ExecutionLogId" INTO v_sync_id FROM "Audit"."ExecutionLog" WHERE "JobID" IN (
	SELECT (_airbyte_meta::json->>'sync_id')::BIGINT
    FROM "stage"."JournalEntryLines"
    LIMIT 1);
    
 -- Insert summary information into TableProcessing, including the ExecutionLogID
    INSERT INTO "Audit"."TableProcessing"
         ("TableName", "ExtractRowCnt", "InsertRowCnt", "UpdateRowCnt", "ExecutionLogID")
    VALUES 
         (
             'Data.JournalEntryLines',
             total_rows_extract,
             total_rows_insert,
             total_rows_update,
             v_sync_id
         ); 	
		 
END;
$BODY$;
ALTER PROCEDURE "Data"."usp_JournalEntryLines"()
    OWNER TO postgres;
