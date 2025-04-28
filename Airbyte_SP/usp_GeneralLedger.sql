-- PROCEDURE: Data.usp_GeneralLedger()

-- DROP PROCEDURE IF EXISTS "Data"."usp_GeneralLedger"();

CREATE OR REPLACE PROCEDURE "Data"."usp_GeneralLedger"(
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
    DROP TABLE IF EXISTS Temp_GeneralLedger;

    -- Create temporary table with the new structure
    CREATE TEMP TABLE Temp_GeneralLedger 
    (
        "TransactionID" INT,
        "AccountID" INT,
        "TransactionDate" TIMESTAMP,
        "Description" VARCHAR(250),
        "Debit" DECIMAL(16,2),
        "Credit" DECIMAL(16,2),
        "Currency" CHAR(20)
    );

    -- Count extracted rows from the stage table
    SELECT COUNT(*) INTO total_rows_extract FROM "stage"."GeneralLedger";
    RAISE NOTICE 'Extracted % rows', total_rows_extract;
IF total_rows_extract>0 THEN
    -- Insert distinct rows into Temp_GeneralLedger using a CTE
    WITH CTE_GeneralLedger AS (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY "TransactionID" ORDER BY "TransactionID") AS rn,
            CAST("TransactionID" AS INT) AS "TransactionID",
            CAST("AccountID" AS INT) AS "AccountID",
            CAST("TransactionDate" AS TIMESTAMP) AS "TransactionDate",
            CAST("Description" AS VARCHAR(250)) AS "Description",
            CAST("Debit" AS DECIMAL(16,2)) AS "Debit",
            CAST("Credit" AS DECIMAL(16,2)) AS "Credit",
            CAST("Currency" AS CHAR(20)) AS "Currency"
        FROM "stage"."GeneralLedger"
    )
    INSERT INTO Temp_GeneralLedger(
        "TransactionID",
        "AccountID",
        "TransactionDate",
        "Description",
        "Debit",
        "Credit",
        "Currency"
    )
    SELECT 
        "TransactionID",
        "AccountID",
        "TransactionDate",
        "Description",
        "Debit",
        "Credit",
        "Currency"
    FROM CTE_GeneralLedger
    WHERE rn = 1;

    -- Insert rows from Temp_GeneralLedger that do not exist in Data.GeneralLedger
    INSERT INTO "Data"."GeneralLedger" (
        "TransactionID",
        "AccountID",
        "TransactionDate",
        "Description",
        "Amount",
        "TransactionTypeId",
        "Currency"
    )
    SELECT 
        CAST(T."TransactionID" AS INT) AS "TransactionID",
        CAST(T."AccountID" AS INT) AS "AccountID",
        CAST(T."TransactionDate" AS TIMESTAMP) AS "TransactionDate",
        CAST(T."Description" AS VARCHAR(250)) AS "Description",
        CASE WHEN CAST(T."Debit" AS DECIMAL(16,2)) > 0 THEN CAST(T."Debit" AS DECIMAL(16,2))
             ELSE CAST(T."Credit" AS DECIMAL(16,2)) END AS "Amount",
        CASE WHEN CAST(T."Debit" AS DECIMAL(16,2)) > 0 THEN 2
             ELSE 1 END AS "TransactionTypeId",
        CAST(T."Currency" AS CHAR(20)) AS "Currency"
    FROM Temp_GeneralLedger T
    LEFT JOIN "Data"."GeneralLedger" BGL 
      ON BGL."TransactionID" = CAST(T."TransactionID" AS INT)
    WHERE BGL."TransactionID" IS NULL;

    GET DIAGNOSTICS total_rows_insert = ROW_COUNT;
    RAISE NOTICE 'Inserted % rows', total_rows_insert;

    -- Update existing rows in Data.GeneralLedger with data from stage.GeneralLedger
    UPDATE "Data"."GeneralLedger" BGL
    SET 
         "AccountID" = CAST(SGL."AccountID" AS INT),
         "TransactionDate" = CAST(SGL."TransactionDate" AS TIMESTAMP),
         "Description" = CAST(SGL."Description" AS VARCHAR(250)),
         "Amount" = CASE WHEN CAST(SGL."Debit" AS DECIMAL(16,2)) > 0 THEN CAST(SGL."Debit" AS DECIMAL(16,2))
                        ELSE CAST(SGL."Credit" AS DECIMAL(16,2)) END,
         "TransactionTypeId" = CASE WHEN CAST(SGL."Debit" AS DECIMAL(16,2)) > 0 THEN 2
                                    WHEN CAST(SGL."Credit" AS DECIMAL(16,2)) > 0 THEN 1
                                    ELSE NULL END,
         "Currency" = CAST(SGL."Currency" AS CHAR(20)),
         "RecordModifiedDate" = CURRENT_TIMESTAMP,
         "RecordModifiedBy" = CURRENT_USER
    FROM "stage"."GeneralLedger" SGL
    WHERE BGL."TransactionID" = CAST(SGL."TransactionID" AS INT);

    GET DIAGNOSTICS total_rows_update = ROW_COUNT;
    RAISE NOTICE 'Updated % rows', total_rows_update;
END IF;
    -- Extract the sync_id from the _airbyte_meta column in stage.GeneralLedger
    SELECT "ExecutionLogId" INTO v_sync_id FROM "Audit"."ExecutionLog" WHERE "JobID" IN (
        SELECT (_airbyte_meta::json->>'sync_id')::BIGINT
        FROM "stage"."GeneralLedger"
        LIMIT 1
    );
    
    -- Insert summary information into TableProcessing, including the ExecutionLogID
    INSERT INTO "Audit"."TableProcessing" 
         ("TableName", "ExtractRowCnt", "InsertRowCnt", "UpdateRowCnt", "ExecutionLogID")
    VALUES 
         (
             'Data.GeneralLedger',
             total_rows_extract,
             total_rows_insert,
             total_rows_update,
             v_sync_id
         );
END;
$BODY$;
ALTER PROCEDURE "Data"."usp_GeneralLedger"()
    OWNER TO postgres;
