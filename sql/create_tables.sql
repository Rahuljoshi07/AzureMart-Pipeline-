/*
 * create_tables.sql
 * Sets up the star schema tables in Azure SQL.
 * Run this before the Gold -> SQL load pipeline.
 */

-- dim_date
-- Calendar table covering several years
IF OBJECT_ID('dbo.dim_date', 'U') IS NOT NULL DROP TABLE dbo.dim_date;

CREATE TABLE dbo.dim_date (
    date_key        INT             NOT NULL PRIMARY KEY,
    full_date       DATE            NOT NULL,
    [year]          INT             NOT NULL,
    [quarter]       INT             NOT NULL,
    [month]         INT             NOT NULL,
    month_name      NVARCHAR(20)    NOT NULL,
    week_of_year    INT             NOT NULL,
    day_of_month    INT             NOT NULL,
    day_of_week     INT             NOT NULL,
    day_name        NVARCHAR(20)    NOT NULL,
    is_weekend      BIT             NOT NULL DEFAULT 0,
    is_holiday      BIT             NOT NULL DEFAULT 0,
    fiscal_year     INT             NULL,
    fiscal_quarter  INT             NULL
);

CREATE INDEX IX_dim_date_full_date ON dbo.dim_date (full_date);
CREATE INDEX IX_dim_date_year_month ON dbo.dim_date ([year], [month]);

-- dim_customer
-- SCD Type 2: tracks segment changes over time
IF OBJECT_ID('dbo.dim_customer', 'U') IS NOT NULL DROP TABLE dbo.dim_customer;

CREATE TABLE dbo.dim_customer (
    customer_sk       BIGINT          NOT NULL PRIMARY KEY,
    customer_id       NVARCHAR(50)    NOT NULL,
    customer_name     NVARCHAR(200)   NOT NULL,
    customer_email    NVARCHAR(200)   NULL,
    customer_segment  NVARCHAR(50)    NOT NULL,
    effective_date    DATE            NOT NULL,
    end_date          DATE            NOT NULL DEFAULT '9999-12-31',
    is_current        BIT             NOT NULL DEFAULT 1
);

CREATE INDEX IX_dim_customer_id ON dbo.dim_customer (customer_id);
CREATE INDEX IX_dim_customer_segment ON dbo.dim_customer (customer_segment);
CREATE INDEX IX_dim_customer_current ON dbo.dim_customer (is_current) WHERE is_current = 1;

-- dim_product
-- Also SCD Type 2 for tracking price changes
IF OBJECT_ID('dbo.dim_product', 'U') IS NOT NULL DROP TABLE dbo.dim_product;

CREATE TABLE dbo.dim_product (
    product_sk      BIGINT          NOT NULL PRIMARY KEY,
    product_id      NVARCHAR(50)    NOT NULL,
    product_name    NVARCHAR(200)   NOT NULL,
    category        NVARCHAR(100)   NOT NULL,
    sub_category    NVARCHAR(100)   NOT NULL,
    brand           NVARCHAR(100)   NOT NULL,
    current_price   DECIMAL(12, 2)  NOT NULL,
    effective_date  DATE            NOT NULL,
    end_date        DATE            NOT NULL DEFAULT '9999-12-31',
    is_current      BIT             NOT NULL DEFAULT 1
);

CREATE INDEX IX_dim_product_id ON dbo.dim_product (product_id);
CREATE INDEX IX_dim_product_category ON dbo.dim_product (category, sub_category);

-- dim_store
IF OBJECT_ID('dbo.dim_store', 'U') IS NOT NULL DROP TABLE dbo.dim_store;

CREATE TABLE dbo.dim_store (
    store_sk        BIGINT          NOT NULL PRIMARY KEY,
    store_id        NVARCHAR(50)    NOT NULL,
    store_name      NVARCHAR(200)   NOT NULL,
    store_city      NVARCHAR(100)   NOT NULL,
    store_state     NVARCHAR(10)    NOT NULL,
    effective_date  DATE            NOT NULL,
    end_date        DATE            NOT NULL DEFAULT '9999-12-31',
    is_current      BIT             NOT NULL DEFAULT 1
);

CREATE INDEX IX_dim_store_id ON dbo.dim_store (store_id);
CREATE INDEX IX_dim_store_state ON dbo.dim_store (store_state);

-- fact_sales
-- The main fact table. FKs to all four dimensions.
IF OBJECT_ID('dbo.fact_sales', 'U') IS NOT NULL DROP TABLE dbo.fact_sales;

CREATE TABLE dbo.fact_sales (
    sales_sk            BIGINT          NOT NULL PRIMARY KEY,
    transaction_id      NVARCHAR(50)    NOT NULL,
    date_key            INT             NOT NULL,
    customer_sk         BIGINT          NOT NULL,
    product_sk          BIGINT          NOT NULL,
    store_sk            BIGINT          NOT NULL,
    quantity            INT             NOT NULL,
    unit_price          DECIMAL(12, 2)  NOT NULL,
    discount_pct        DECIMAL(5, 4)   NOT NULL DEFAULT 0,
    total_amount        DECIMAL(14, 2)  NOT NULL,
    net_amount          DECIMAL(14, 2)  NOT NULL,
    payment_method      NVARCHAR(50)    NOT NULL,
    order_status        NVARCHAR(50)    NOT NULL,
    processing_timestamp DATETIME2      NOT NULL DEFAULT GETUTCDATE(),

    -- Foreign keys
    CONSTRAINT FK_fact_sales_date     FOREIGN KEY (date_key)     REFERENCES dbo.dim_date(date_key),
    CONSTRAINT FK_fact_sales_customer FOREIGN KEY (customer_sk)  REFERENCES dbo.dim_customer(customer_sk),
    CONSTRAINT FK_fact_sales_product  FOREIGN KEY (product_sk)   REFERENCES dbo.dim_product(product_sk),
    CONSTRAINT FK_fact_sales_store    FOREIGN KEY (store_sk)     REFERENCES dbo.dim_store(store_sk)
);

-- Indexes for the query patterns we'll use most
CREATE INDEX IX_fact_sales_date      ON dbo.fact_sales (date_key);
CREATE INDEX IX_fact_sales_customer  ON dbo.fact_sales (customer_sk);
CREATE INDEX IX_fact_sales_product   ON dbo.fact_sales (product_sk);
CREATE INDEX IX_fact_sales_store     ON dbo.fact_sales (store_sk);
CREATE INDEX IX_fact_sales_status    ON dbo.fact_sales (order_status);
CREATE INDEX IX_fact_sales_txn_id    ON dbo.fact_sales (transaction_id);

-- This covering index helps aggregation queries avoid key lookups
CREATE INDEX IX_fact_sales_agg ON dbo.fact_sales (date_key, store_sk, product_sk)
    INCLUDE (net_amount, quantity);

-- pipeline_log
-- Keeps track of pipeline runs, steps, timings, and errors
IF OBJECT_ID('dbo.pipeline_log', 'U') IS NOT NULL DROP TABLE dbo.pipeline_log;

CREATE TABLE dbo.pipeline_log (
    log_id          INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name   NVARCHAR(100)   NOT NULL,
    run_id          NVARCHAR(50)    NOT NULL,
    step_name       NVARCHAR(100)   NOT NULL,
    status          NVARCHAR(20)    NOT NULL,  -- SUCCESS, FAILED, WARNING
    row_count       INT             NULL,
    error_message   NVARCHAR(MAX)   NULL,
    start_time      DATETIME2       NOT NULL,
    end_time        DATETIME2       NULL,
    duration_sec    INT             NULL
);

PRINT 'All tables created.';
GO
