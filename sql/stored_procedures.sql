/*
 * stored_procedures.sql
 * Procs used by ADF for loading dimensions and facts.
 */

-- Generic dimension upsert (SCD Type 2 via MERGE)
CREATE OR ALTER PROCEDURE dbo.sp_upsert_dimension
    @table_name NVARCHAR(100),
    @staging_table NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @start DATETIME2 = GETUTCDATE();
    DECLARE @row_count INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Dynamic MERGE based on table name
        IF @table_name = 'dim_customer'
        BEGIN
            MERGE dbo.dim_customer AS target
            USING (SELECT * FROM dbo.stg_dim_customer) AS source
            ON target.customer_id = source.customer_id AND target.is_current = 1
            WHEN MATCHED AND (
                target.customer_name != source.customer_name OR
                target.customer_email != source.customer_email OR
                target.customer_segment != source.customer_segment
            ) THEN
                UPDATE SET
                    is_current = 0,
                    end_date = CAST(GETUTCDATE() AS DATE)
            WHEN NOT MATCHED THEN
                INSERT (customer_sk, customer_id, customer_name, customer_email,
                        customer_segment, effective_date, end_date, is_current)
                VALUES (source.customer_sk, source.customer_id, source.customer_name,
                        source.customer_email, source.customer_segment,
                        CAST(GETUTCDATE() AS DATE), '9999-12-31', 1);

            SET @row_count = @@ROWCOUNT;
        END

        COMMIT TRANSACTION;

        -- Log success
        INSERT INTO dbo.pipeline_log (pipeline_name, run_id, step_name, status, row_count, start_time, end_time, duration_sec)
        VALUES ('DimensionLoad', CONVERT(NVARCHAR(50), NEWID()), @table_name, 'SUCCESS', @row_count,
                @start, GETUTCDATE(), DATEDIFF(SECOND, @start, GETUTCDATE()));

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

        INSERT INTO dbo.pipeline_log (pipeline_name, run_id, step_name, status, error_message, start_time, end_time, duration_sec)
        VALUES ('DimensionLoad', CONVERT(NVARCHAR(50), NEWID()), @table_name, 'FAILED',
                ERROR_MESSAGE(), @start, GETUTCDATE(), DATEDIFF(SECOND, @start, GETUTCDATE()));

        THROW;
    END CATCH
END;
GO

-- Fact sales loader (incremental with delete+insert for idempotency)
CREATE OR ALTER PROCEDURE dbo.sp_load_fact_sales
    @date_from INT = NULL,
    @date_to INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start DATETIME2 = GETUTCDATE();
    DECLARE @row_count INT;
    DECLARE @run_id NVARCHAR(50) = CONVERT(NVARCHAR(50), NEWID());

    BEGIN TRY
        -- If no date range provided, load last 7 days
        IF @date_from IS NULL
            SET @date_from = CAST(FORMAT(DATEADD(DAY, -7, GETUTCDATE()), 'yyyyMMdd') AS INT);
        IF @date_to IS NULL
            SET @date_to = CAST(FORMAT(GETUTCDATE(), 'yyyyMMdd') AS INT);

        BEGIN TRANSACTION;

        -- Delete existing records for the date range (idempotent reload)
        DELETE FROM dbo.fact_sales
        WHERE date_key BETWEEN @date_from AND @date_to;

        -- Insert from staging
        INSERT INTO dbo.fact_sales (
            sales_sk, transaction_id, date_key, customer_sk, product_sk, store_sk,
            quantity, unit_price, discount_pct, total_amount, net_amount,
            payment_method, order_status, processing_timestamp
        )
        SELECT
            sales_sk, transaction_id, date_key, customer_sk, product_sk, store_sk,
            quantity, unit_price, discount_pct, total_amount, net_amount,
            payment_method, order_status, processing_timestamp
        FROM dbo.stg_fact_sales
        WHERE date_key BETWEEN @date_from AND @date_to;

        SET @row_count = @@ROWCOUNT;

        COMMIT TRANSACTION;

        -- Log
        INSERT INTO dbo.pipeline_log (pipeline_name, run_id, step_name, status, row_count, start_time, end_time, duration_sec)
        VALUES ('FactLoad', @run_id, 'fact_sales', 'SUCCESS', @row_count,
                @start, GETUTCDATE(), DATEDIFF(SECOND, @start, GETUTCDATE()));

        SELECT @row_count AS rows_loaded, 'SUCCESS' AS status;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

        INSERT INTO dbo.pipeline_log (pipeline_name, run_id, step_name, status, error_message, start_time, end_time, duration_sec)
        VALUES ('FactLoad', @run_id, 'fact_sales', 'FAILED', ERROR_MESSAGE(),
                @start, GETUTCDATE(), DATEDIFF(SECOND, @start, GETUTCDATE()));

        THROW;
    END CATCH
END;
GO

-- Data quality checks - run after each load to catch issues
CREATE OR ALTER PROCEDURE dbo.sp_run_quality_checks
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @results TABLE (
        check_name NVARCHAR(200),
        check_result NVARCHAR(20),
        details NVARCHAR(500)
    );

    -- Check 1: Orphan fact records (no matching dimension)
    INSERT INTO @results
    SELECT 'Orphan customer_sk in fact_sales', 
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           CAST(COUNT(*) AS NVARCHAR) + ' orphan records'
    FROM dbo.fact_sales f
    LEFT JOIN dbo.dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.customer_sk IS NULL AND f.customer_sk != -1;

    -- Check 2: Negative amounts
    INSERT INTO @results
    SELECT 'Negative net_amount in fact_sales',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END,
           CAST(COUNT(*) AS NVARCHAR) + ' records with negative amounts'
    FROM dbo.fact_sales
    WHERE net_amount < 0;

    -- Check 3: Future dates
    INSERT INTO @results
    SELECT 'Future dates in fact_sales',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END,
           CAST(COUNT(*) AS NVARCHAR) + ' records with future dates'
    FROM dbo.fact_sales f
    JOIN dbo.dim_date d ON f.date_key = d.date_key
    WHERE d.full_date > GETUTCDATE();

    -- Check 4: Row count sanity
    INSERT INTO @results
    SELECT 'fact_sales row count',
           CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
           CAST(COUNT(*) AS NVARCHAR) + ' total rows'
    FROM dbo.fact_sales;

    SELECT * FROM @results;
END;
GO

PRINT 'Stored procedures created.';
GO
