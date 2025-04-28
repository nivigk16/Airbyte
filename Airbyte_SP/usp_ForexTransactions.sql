-- PROCEDURE: Data.usp_ForexTransactions()

-- DROP PROCEDURE IF EXISTS "Data"."usp_ForexTransactions"();

CREATE OR REPLACE PROCEDURE "Data"."usp_ForexTransactions"(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
    total_rows_insert integer;
    total_rows_update integer;
    total_rows_extract integer;
	v_sync_id BIGINT;
BEGIN
    -- Drop temp table if exists
    DROP TABLE IF EXISTS Temp_ForexTransactions;

    -- Create temporary table
    CREATE TEMP TABLE Temp_ForexTransactions 
    (
        "TransactionID" INT,
        "TransactionDate" TIMESTAMP,
        "TransactionType" VARCHAR(50),
        "CustomerID" INT,
        "Amount" DECIMAL(16,2),
        "MerchantName" VARCHAR(150),
        "Currency" VARCHAR(20)
    );
    
    -- Extract row count
    SELECT COUNT(*) INTO total_rows_extract FROM "stage"."ForexTransactions";
    RAISE NOTICE 'Extracted % rows', total_rows_extract;
	
IF total_rows_extract>0 THEN
    -- Insert unique transactions into temporary table
    WITH CTE_ForexTransactions AS (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY "Transaction_ID" ORDER BY "Transaction_ID") AS rn,
            CAST("Transaction_ID" AS INT) AS "TransactionID",
            CAST("Transaction_Date" AS TIMESTAMP) AS "TransactionDate",
            CAST("Transaction_Type" AS VARCHAR(50)) AS "TransactionType",
            CAST("Customer_ID" AS INT) AS "CustomerID",
            CASE 
                WHEN "Amount_Value" IS NOT NULL AND "Amount_Value" != '' 
                THEN CAST("Amount_Value" AS DECIMAL(16,2)) 
                ELSE NULL 
            END AS "Amount",
            CAST("Merchant_Name" AS VARCHAR(150)) AS "MerchantName",
            CAST("Currency" AS VARCHAR(20)) AS "Currency"
        FROM "stage"."ForexTransactions"
    )
    INSERT INTO Temp_ForexTransactions(
        "TransactionID", "TransactionDate", "TransactionType", "MerchantName",
        "CustomerID", "Amount", "Currency"
    )
    SELECT "TransactionID", "TransactionDate", "TransactionType", "MerchantName",
           "CustomerID", "Amount", "Currency"
    FROM CTE_ForexTransactions
    WHERE rn = 1;

    -- Insert new transactions into main table
    INSERT INTO "Data"."ForexTransactions" (
        "TransactionID", "TransactionDate", "TransactionType", "MerchantName",
        "CustomerID", "Amount", "Currency", "EquivalentUSDValue"
    )
    SELECT 
        T."TransactionID", T."TransactionDate", T."TransactionType", T."MerchantName",
        T."CustomerID", T."Amount", T."Currency",
        CASE 
            WHEN T."Amount" IS NOT NULL 
            THEN CAST(T."Amount" AS DECIMAL(16,2)) * BCR."usdvalue" 
            ELSE NULL 
        END
    FROM Temp_ForexTransactions T
    LEFT JOIN "Data"."ForexTransactions" BFX ON BFX."TransactionID" = T."TransactionID"
    LEFT JOIN "DIM"."Currency" BCR ON T."Currency" = BCR."currency"
    WHERE BFX."TransactionID" IS NULL;
    
    -- Capture insert row count
    GET DIAGNOSTICS total_rows_insert = ROW_COUNT;
    RAISE NOTICE 'Inserted % rows', total_rows_insert;

    -- Update existing transactions

	UPDATE "Data"."ForexTransactions" BFX
	SET 
		"TransactionDate"    = CAST(SFX."Transaction_Date" AS TIMESTAMP),
		"TransactionType"    = SFX."Transaction_Type",
		"MerchantName"       = SFX."Merchant_Name",
		"CustomerID"         = CAST(SFX."Customer_ID" AS INT),
		"Amount"             = CASE 
								  WHEN SFX."Amount_Value" IS NOT NULL 
									   AND SFX."Amount_Value" != '' 
								  THEN CAST(SFX."Amount_Value" AS DECIMAL(16,2)) 
								  ELSE NULL 
							   END,
		"Currency"           = SFX."Currency",
		"EquivalentUSDValue" = CASE 
								  WHEN SFX."Amount_Value" IS NOT NULL 
									   AND SFX."Amount_Value" != '' 
								  THEN CAST(SFX."Amount_Value" AS DECIMAL(16,2)) * BCR."usdvalue" 
								  ELSE NULL 
							   END
	FROM "stage"."ForexTransactions" SFX
	LEFT JOIN "DIM"."Currency" BCR ON SFX."Currency" = BCR."currency"
	WHERE BFX."TransactionID" = CAST(SFX."Transaction_ID" AS INT);
	
	    GET DIAGNOSTICS total_rows_update = ROW_COUNT;
	    RAISE NOTICE 'Updated % rows', total_rows_update;

END IF;
	-- Extract the sync_id from the _airbyte_meta column in stage.CashFlow
	    SELECT "ExecutionLogId" INTO v_sync_id FROM "Audit"."ExecutionLog" WHERE "JobID" IN (
		SELECT (_airbyte_meta::json->>'sync_id')::BIGINT
	    FROM "stage"."ForexTransactions"
	    LIMIT 1);
	    
	 -- Insert summary information into TableProcessing, including the ExecutionLogID
	    INSERT INTO "Audit"."TableProcessing" 
	         ("TableName", "ExtractRowCnt", "InsertRowCnt", "UpdateRowCnt", "ExecutionLogID")
	    VALUES 
	         (
	             'Data.ForexTransactions',
	             total_rows_extract,
	             total_rows_insert,
	             total_rows_update,
	             v_sync_id
	         ); 
END;
$BODY$;
ALTER PROCEDURE "Data"."usp_ForexTransactions"()
    OWNER TO postgres;
