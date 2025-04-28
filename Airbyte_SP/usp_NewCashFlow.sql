-- PROCEDURE: Data.usp_NewCashFlow()

-- DROP PROCEDURE IF EXISTS "Data"."usp_NewCashFlow"();

CREATE OR REPLACE PROCEDURE "Data"."usp_NewCashFlow"(
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
    DROP TABLE IF EXISTS Temp_NewCashFlow;

    -- Create temporary table with the new structure
    CREATE TEMP TABLE Temp_NewCashFlow 
	(
		"Date" TIMESTAMP,
		"TransactionID" VARCHAR(20),
		"AccountID" VARCHAR(20),
		"CashFlowCategory" VARCHAR(20),
		"ActivityDescription" VARCHAR(20),
		"InflowAmount" DECIMAL(16,2),
		"OutflowAmount" DECIMAL(16,2),
		"NetCashFlow" DECIMAL(16,2)
	);

    -- Count extracted rows from the stage table
    SELECT COUNT(*) INTO total_rows_extract FROM "stage"."NewCashFlow";
    RAISE NOTICE 'Extracted % rows', total_rows_extract;
IF total_rows_extract>0 THEN
    -- Insert distinct rows into Temp_NewCashFlow using a CTE
	WITH CTE_NewCashFlow AS (
	    SELECT 
	        ROW_NUMBER() OVER (PARTITION BY "Transaction_ID" ORDER BY "Transaction_ID") AS rn,
	        CAST("Date" AS TIMESTAMP) AS "Date",
	        CAST("Transaction_ID" AS VARCHAR(20)) AS "TransactionID",
	        CAST("Account_ID" AS VARCHAR(20)) AS "AccountID",
	        CAST("Cash_Flow_Category" AS VARCHAR(20)) AS "CashFlowCategory",
	        CAST("Activity_Description" AS VARCHAR(20)) AS "ActivityDescription",
	        CAST("Inflow_Amount" AS DECIMAL(16,2)) AS "InflowAmount",
	        CAST("Outflow_Amount" AS DECIMAL(16,2)) AS "OutflowAmount",
	        CAST("Net_Cash_Flow" AS DECIMAL(16,2)) AS "NetCashFlow"
	    FROM "stage"."NewCashFlow"
	)

    INSERT INTO Temp_NewCashFlow(
        "Date",	
        "TransactionID",	
        "AccountID",	
        "CashFlowCategory",	
        "ActivityDescription",	
        "InflowAmount",	
        "OutflowAmount",	
        "NetCashFlow"
    )
    SELECT 
        "Date",	
        "TransactionID",	
        "AccountID",	
        "CashFlowCategory",	
        "ActivityDescription",	
        "InflowAmount",	
        "OutflowAmount",	
        "NetCashFlow"
    FROM CTE_NewCashFlow 
    WHERE rn = 1;

    -- Insert rows from Temp_NewCashFlow that do not exist in Data.NewCashFlow
    INSERT INTO "Data"."NewCashFlow" (
         "Date",	
         "TransactionID",	
         "AccountID",	
         "CashFlowCategory",	
         "ActivityDescription",	
         "InflowAmount",	
         "OutflowAmount",	
         "NetCashFlow"
    )
    SELECT 
         T."Date",
         T."TransactionID",
         T."AccountID",
         T."CashFlowCategory",
         T."ActivityDescription",
         T."InflowAmount",
         T."OutflowAmount",
         T."NetCashFlow"
    FROM Temp_NewCashFlow T
    LEFT JOIN "Data"."NewCashFlow" BCF 
      ON BCF."TransactionID" = T."TransactionID"
    WHERE BCF."TransactionID" IS NULL;

    GET DIAGNOSTICS total_rows_insert = ROW_COUNT;
    RAISE NOTICE 'Inserted % rows', total_rows_insert;

    -- Update existing rows in Data.NewCashFlow with data from Temp_NewCashFlow
    UPDATE "Data"."NewCashFlow" BCF
    SET 
         "Date"                = T."Date",
         "AccountID"           = T."AccountID",
         "CashFlowCategory"    = T."CashFlowCategory",
         "ActivityDescription" = T."ActivityDescription",
         "InflowAmount"        = T."InflowAmount",
         "OutflowAmount"       = T."OutflowAmount",
         "NetCashFlow"         = T."NetCashFlow",
		 "RecordModifiedDate"  = CURRENT_TIMESTAMP,
		 "RecordModifiedBy"    = CURRENT_USER
    FROM Temp_NewCashFlow T
    WHERE BCF."TransactionID" = T."TransactionID";
BEGIN
    GET DIAGNOSTICS total_rows_update = ROW_COUNT;
    RAISE NOTICE 'Updated % rows', total_rows_update;
END;
END IF;
-- Extract the sync_id from the _airbyte_meta column in stage.CashFlow
    SELECT "ExecutionLogId" INTO v_sync_id FROM "Audit"."ExecutionLog" WHERE "JobID" IN (
	SELECT (_airbyte_meta::json->>'sync_id')::BIGINT
    FROM "stage"."NewCashFlow"
    LIMIT 1);
    
 -- Insert summary information into TableProcessing, including the ExecutionLogID
    INSERT INTO "Audit"."TableProcessing" 
         ("TableName", "ExtractRowCnt", "InsertRowCnt", "UpdateRowCnt", "ExecutionLogID")
    VALUES 
         (
             'Data.NewCashFlow',
             total_rows_extract,
             total_rows_insert,
             total_rows_update,
             v_sync_id
         );    
END;
$BODY$;
ALTER PROCEDURE "Data"."usp_NewCashFlow"()
    OWNER TO postgres;
