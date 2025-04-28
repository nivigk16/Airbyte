-- PROCEDURE: Data.usp_ProfitAndLoss()

-- DROP PROCEDURE IF EXISTS "Data"."usp_ProfitAndLoss"();

CREATE OR REPLACE PROCEDURE "Data"."usp_ProfitAndLoss"(
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
    DROP TABLE IF EXISTS Temp_ProfitAndLoss;
    
    -- Create temporary table including new columns for MonthName, Year, and Quarter
    CREATE TEMP TABLE Temp_ProfitAndLoss
    (
        "Date" TIMESTAMP,
        "AccountID" VARCHAR(20),
        "Amount" DECIMAL(16,2),
        "TransactionID" VARCHAR(20),
        "DepartmentOrCostCenter" VARCHAR(250),
        "MonthName" VARCHAR(20),  -- New column for Month Name
        "Year" INT,                -- New column for Year
        "Quarter" INT              -- New column for Quarter
    );

    -- Count the total rows extracted from the staging table
BEGIN	
    SELECT COUNT(*) INTO total_rows_extract FROM "stage"."ProfitAndLoss";
    RAISE NOTICE 'Extracted % rows', total_rows_extract;
END;	
IF total_rows_extract>0 THEN
    -- Use a CTE to filter duplicates and prepare data for insertion
    WITH CTE_ProfitAndLoss AS (
        SELECT 
            ROW_NUMBER() OVER (
                PARTITION BY "Transaction_ID"
                ORDER BY "Transaction_ID"
            ) AS rn,
            CAST("Date" AS TIMESTAMP) AS "Date",  -- Cast to TIMESTAMP
            CAST("Account_ID" AS VARCHAR(20)) AS "AccountID",
            CAST("Amount" AS DECIMAL(16,2)) AS "Amount",
            CAST("Transaction_ID" AS VARCHAR(20)) AS "TransactionID",
            CAST("Department_Cost_Center" AS VARCHAR(250)) AS "DepartmentOrCostCenter",
            TO_CHAR(CAST("Date" AS TIMESTAMP), 'Month') AS "MonthName",  -- Extract Month Name
            EXTRACT(YEAR FROM CAST("Date" AS TIMESTAMP)) AS "Year",      -- Extract Year
            EXTRACT(QUARTER FROM CAST("Date" AS TIMESTAMP)) AS "Quarter"  -- Extract Quarter
        FROM "stage"."ProfitAndLoss"
    )
    INSERT INTO Temp_ProfitAndLoss("Date", "AccountID", "Amount", "TransactionID", "DepartmentOrCostCenter", "MonthName", "Year", "Quarter")
    SELECT 
        "Date", 
        "AccountID", 
        "Amount", 
        "TransactionID", 
        "DepartmentOrCostCenter",
        "MonthName",
        "Year",
        "Quarter"
    FROM CTE_ProfitAndLoss 
    WHERE rn = 1;

    -- Insert rows from Temp_ProfitAndLoss that do not exist in Data.ProfitAndLoss
    INSERT INTO "Data"."ProfitAndLoss" (
        "Date", 
        "AccountID", 
        "Amount", 
        "TransactionID", 
        "DepartmentOrCostCenter",
        "MonthName",  -- Include Month Name in insert
        "Year",       -- Include Year in insert
        "Quarter"     -- Include Quarter in insert
    )
    SELECT 
        T."Date",
        T."AccountID",
        T."Amount",
        T."TransactionID",
        T."DepartmentOrCostCenter",
        T."MonthName",  -- Include Month Name from temp table
        T."Year",       -- Include Year from temp table
        T."Quarter"     -- Include Quarter from temp table
    FROM Temp_ProfitAndLoss T
    LEFT JOIN "Data"."ProfitAndLoss" BPL 
        ON BPL."TransactionID" = T."TransactionID"
    WHERE BPL."TransactionID" IS NULL;

    -- Get the count of inserted rows
BEGIN	
    GET DIAGNOSTICS total_rows_insert = ROW_COUNT;
    RAISE NOTICE 'Inserted % rows', total_rows_insert;
END;

    -- Update existing rows in Data.ProfitAndLoss with data from stage.ProfitAndLoss
    UPDATE "Data"."ProfitAndLoss" BPL
    SET 
        "Date" = T."Date",
        "AccountID" = T."AccountID",
        "Amount" = T."Amount",
        "DepartmentOrCostCenter" = T."DepartmentOrCostCenter",
        "MonthName" = T."MonthName",  -- Update Month Name
        "Year" = T."Year",             -- Update Year
        "Quarter" = T."Quarter",       -- Update Quarter
        "RecordModifiedDate" = CURRENT_TIMESTAMP,
        "RecordModifiedBy" = CURRENT_USER
    FROM Temp_ProfitAndLoss T
    WHERE BPL."TransactionID" = T."TransactionID";

    -- Get the count of updated rows
BEGIN	
    GET DIAGNOSTICS total_rows_update = ROW_COUNT;
    RAISE NOTICE 'Updated % rows', total_rows_update;
END;
END IF;
-- Extract the sync_id from the _airbyte_meta column in stage.CashFlow
    SELECT "ExecutionLogId" INTO v_sync_id FROM "Audit"."ExecutionLog" WHERE "JobID" IN (
	SELECT (_airbyte_meta::json->>'sync_id')::BIGINT
    FROM "stage"."ProfitAndLoss"
    LIMIT 1);

   -- Insert summary information into TableProcessing, including the ExecutionLogID
    INSERT INTO "Audit"."TableProcessing"
         ("TableName", "ExtractRowCnt", "InsertRowCnt", "UpdateRowCnt", "ExecutionLogID")
    VALUES 
         (
             'Data.ProfitAndLoss',
             total_rows_extract,
             total_rows_insert,
             total_rows_update,
             v_sync_id
         );
END;
$BODY$;
ALTER PROCEDURE "Data"."usp_ProfitAndLoss"()
    OWNER TO postgres;
