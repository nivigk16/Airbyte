-- PROCEDURE: Data.usp_BalanceSheet()

-- DROP PROCEDURE IF EXISTS "Data"."usp_BalanceSheet"();

CREATE OR REPLACE PROCEDURE "Data"."usp_BalanceSheet"(
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
    DROP TABLE IF EXISTS Temp_BalanceSheet;

    -- Create temporary table with the new structure
    CREATE TEMP TABLE Temp_BalanceSheet
    (
        "Date" TIMESTAMP,
        "AccountID" VARCHAR(50),
        "OpeningBalance" DECIMAL(16,2),
        "ClosingBalance" DECIMAL(16,2),
        "CurrentOrNoncurrent" VARCHAR(50)
    );

    -- Count extracted rows from the stage table
    SELECT COUNT(*) INTO total_rows_extract FROM "stage"."BalanceSheet";
    RAISE NOTICE 'Extracted % rows', total_rows_extract;
IF total_rows_extract>0 THEN
    -- Insert distinct rows into Temp_BalanceSheet using a CTE
	WITH CTE_BalanceSheet AS (
	    SELECT 
	        ROW_NUMBER() OVER (PARTITION BY "Account_ID", "Date" ORDER BY "Date") AS rn,
	        CAST("Date" AS TIMESTAMP) AS "Date",
	        CAST("Account_ID" AS VARCHAR(50)) AS "AccountID",  -- Fix: "Account_ID" instead of "AccountID"
	        CAST("Opening_Balance" AS DECIMAL(16,2)) AS "OpeningBalance",
	        CAST("Closing_Balance" AS DECIMAL(16,2)) AS "ClosingBalance",
	        CAST("Current_Non_current_Indicator" AS VARCHAR(50)) AS "CurrentOrNoncurrent"
	    FROM "stage"."BalanceSheet"
	)

    INSERT INTO Temp_BalanceSheet(
        "Date",
        "AccountID",
        "OpeningBalance",
        "ClosingBalance",
        "CurrentOrNoncurrent"
    )
    SELECT 
        "Date", 
        "AccountID", 
        "OpeningBalance", 
        "ClosingBalance", 
        "CurrentOrNoncurrent"
    FROM CTE_BalanceSheet 
    WHERE rn = 1;

    -- Insert rows from Temp_BalanceSheet that do not exist in Data.BalanceSheet
    INSERT INTO "Data"."BalanceSheet" (
         "Date", 
         "AccountID", 
         "OpeningBalance", 
         "ClosingBalance", 
         "CurrentOrNoncurrent"
    )
    SELECT 
         T."Date",
         T."AccountID",
         T."OpeningBalance",
         T."ClosingBalance",
         T."CurrentOrNoncurrent"
    FROM Temp_BalanceSheet T
    LEFT JOIN "Data"."BalanceSheet" BCF 
      ON BCF."AccountID" = T."AccountID"
      AND BCF."Date" = T."Date"
    WHERE BCF."AccountID" IS NULL;

    GET DIAGNOSTICS total_rows_insert = ROW_COUNT;
    RAISE NOTICE 'Inserted % rows', total_rows_insert;

    -- Update existing rows in Data.BalanceSheet with data from stage.BalanceSheet
    UPDATE "Data"."BalanceSheet" BCF
    SET 
         "OpeningBalance" = T."OpeningBalance",
         "ClosingBalance" = T."ClosingBalance",
         "CurrentOrNoncurrent" = T."CurrentOrNoncurrent",
		 "RecordModifiedDate" = CURRENT_TIMESTAMP,
		 "RecordModifiedBy" = CURRENT_USER
    FROM Temp_BalanceSheet T
    WHERE BCF."AccountID" = T."AccountID"
      AND BCF."Date" = T."Date";

    BEGIN
         GET DIAGNOSTICS total_rows_update = ROW_COUNT;
         RAISE NOTICE 'Updated % rows', total_rows_update;
    END;
END IF;
    -- Extract the sync_id from the _airbyte_meta column in stage.CashFlow
    SELECT "ExecutionLogId" INTO v_sync_id FROM "Audit"."ExecutionLog" WHERE "JobID" IN (
	SELECT (_airbyte_meta::json->>'sync_id')::BIGINT
    FROM "stage"."BalanceSheet"
    LIMIT 1);
    
    -- Insert summary information into TableProcessing, including the ExecutionLogID
    INSERT INTO "Audit"."TableProcessing" 
         ("TableName", "ExtractRowCnt", "InsertRowCnt", "UpdateRowCnt", "ExecutionLogID")
    VALUES 
         (
             'Data.BalanceSheet',
             total_rows_extract,
             total_rows_insert,
             total_rows_update,
             v_sync_id
         );
END;
$BODY$;
ALTER PROCEDURE "Data"."usp_BalanceSheet"()
    OWNER TO postgres;
