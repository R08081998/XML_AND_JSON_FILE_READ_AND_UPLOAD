use pr

CREATE TABLE emp_table (
    EmployeeID INT,
    Department VARCHAR(50),
    Salary DECIMAL(18, 2)
);


CREATE TABLE SELLER 
(
id BIGINT,
first_name VARCHAR(100),
last_name VARCHAR(100),
email VARCHAR(MAX),
gender VARCHAR(30),
country VARCHAR(100)
);

INSERT INTO SELLER (id, first_name, last_name, email, gender, country)

SELECT TOP 10
MY_XML.record.query ('id' ) . value ('.', 'BIGINT' ),
MY_XML.record.query ('first_name' ) . value ('.', 'VARCHAR (100)'),
MY_XML.record.query ('last_name' ) . value ('.', 'VARCHAR (100)'),
MY_XML.record.query ('email' ) . value ('.', 'VARCHAR (MAX)' ),
MY_XML.record.query ('gender' ) . value ('.', 'VARCHAR (30)' ),
MY_XML.record.query ('country' ) . value ('.', 'VARCHAR (100)' )

FROM 
     (SELECT CAST (X AS xml)
          FROM OPENROWSET (BULK 'C:\Users\ASUS\Downloads\Sellers.xml', SINGLE_BLOB) AS T(X) ) AS T (Y)
CROSS APPLY T.Y. nodes ('dataset/record' ) AS MY_XML (record) ;


SELECT * FROM SELLER

select * from update_table for xml PATH('RECORD') ,ROOT('DATASET');

-------------------------------------------------------------------------------------------------------------------
---this query show onlay Column Name--

SELECT DISTINCT 
    COLUMN_NAME = C.value('local-name(.)', 'NVARCHAR(255)')
FROM OPENROWSET(
    BULK 'C:\Users\ASUS\Downloads\Sellers.xml', 
    SINGLE_BLOB
) AS DataFile
CROSS APPLY (SELECT CAST(DataFile.BulkColumn AS XML) AS XmlData) AS X
CROSS APPLY X.XmlData.nodes('/*/*[1]/*') AS T(C);
-------------------------------------------------------------------------------------------------------------------

SELECT TOP 10
    MY_XML.record.query('id').value('.', 'BIGINT'),
    MY_XML.record.query('first_name').value('.', 'VARCHAR(100)'),
    MY_XML.record.query('last_name').value('.', 'VARCHAR(100)'),
    MY_XML.record.query('email').value('.', 'VARCHAR(MAX)'),
    MY_XML.record.query('gender').value('.', 'VARCHAR(30)'),
    MY_XML.record.query('country').value('.', 'VARCHAR(100)')
FROM
    (SELECT CAST(X AS xml)
     FROM OPENROWSET(BULK 'C:\Users\ASUS\Downloads\Sellers.xml', SINGLE_BLOB) AS T(X)) AS T(Y)
CROSS APPLY T.Y.nodes('dataset/record') AS MY_XML(record);




-------------------------------------------------------------------------------------------------------------------
SELECT * FROM update_table as b cross apply Employees

SELECT * FROM SELLER FOR XML PATH('ALL_RECORD') , ROOT('ONE_DATASET')-------
SELECT * FROM SELLER FOR JSON PATH , ROOT('ONE_DATASET')-------


-------------------------------------------------------------------------------------------------------------------
-----for json

SELECT DISTINCT [key] AS ColumnName
FROM OPENROWSET(
    BULK 'C:\Users\ASUS\Downloads\csvjson.json',
    SINGLE_CLOB
) AS DataFile
CROSS APPLY OPENJSON(BulkColumn, '$[0]');
 -- Target the first object in the array
 ---------------------------------------ANOTHER WAY-----------------------------
 


-------------------------------------------------------------------------------------------------------------------

SELECT *
FROM OPENROWSET(
    BULK 'D:\path_to_file\parquet_file.parquet',
    DATA_SOURCE = 'ExternalDataSource',
    FORMAT = 'PARQUET'
) AS DataFile
WHERE 1 = 0; -- This prevents loading the data, returning only column names

SELECT *
FROM OPENROWSET(
    BULK 'C:\Users\ASUS\Downloads\export_1739948130419.parquet',
    FORMAT = 'PARQUET'
) AS DataFile
WHERE 1 = 0;
----


CREATE EXTERNAL TABLE EmployeeData (
    EmployeeID INT,
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50)
)
WITH (
    LOCATION = 'D:/file/Parquet_DATA.parquet',
    DATA_SOURCE = MyDataSource,
    FILE_FORMAT = MyParquetFormat
);

SELECT * FROM EmployeeData;

-------------------------------------------------------------------------------------------------------------------
--------------------XML CODE FILE----------------------------------
-------------------------------------------------------------------------------------------------------------------
---TWO METHOD IS CORRECT TO READ XML FILE IN SQL SERVER----------------------------
--1...DIRECT CODE

SELECT TOP 10
    MY_XML.record.query('id').value('.', 'BIGINT'),
    MY_XML.record.query('first_name').value('.', 'VARCHAR(100)'),
    MY_XML.record.query('last_name').value('.', 'VARCHAR(100)'),
    MY_XML.record.query('email').value('.', 'VARCHAR(MAX)'),
    MY_XML.record.query('gender').value('.', 'VARCHAR(30)'),
    MY_XML.record.query('country').value('.', 'VARCHAR(100)')
FROM
    (SELECT CAST(X AS xml)
     FROM OPENROWSET(BULK 'C:\Users\ASUS\Downloads\Sellers.xml', SINGLE_BLOB) AS T(X)) AS T(Y)
CROSS APPLY T.Y.nodes('dataset/record') AS MY_XML(record);
------------------------------------------------------------------
------------------------------------------------------------------
--2... DECLARE CODE                      ----------   1 & 2 CODE WORKING PERFECTALY

DECLARE @XML_DATA XML;
-- Read XML data from the file
SELECT @XML_DATA = CAST(BULKCOLUMN AS XML)
FROM OPENROWSET
(
    BULK 'C:\Users\ASUS\Downloads\Sellers.xml', SINGLE_BLOB
) AS DATA_SOURCE;
-- Parse the XML data and extract the information from the "record" nodes
INSERT INTO SELLER (id, first_name, last_name, email, gender, country)
SELECT 
    T.X.value('(id)[1]', 'BIGINT') AS id,
    T.X.value('(first_name)[1]', 'VARCHAR(100)') AS first_name,
    T.X.value('(last_name)[1]', 'VARCHAR(100)') AS last_name,
    T.X.value('(email)[1]', 'VARCHAR(MAX)') AS email,
    T.X.value('(gender)[1]', 'VARCHAR(30)') AS gender,
    T.X.value('(country)[1]', 'VARCHAR(100)') AS country
FROM @XML_DATA.nodes('dataset/record') AS T(X);

------------------------------------------------------------------------------------------------
DECLARE @XML_DATA XML;

-- Read XML data from the file
SELECT @XML_DATA = CAST(BULKCOLUMN AS XML)
FROM OPENROWSET(
    BULK 'C:\Users\ASUS\Downloads\Sellers.xml',
    SINGLE_BLOB
) AS DATA_SOURCE;

-- Parse the XML data into a temporary table
CREATE TABLE #TEMP_SELLER (
    id BIGINT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(MAX),
    gender VARCHAR(30),
    country VARCHAR(100)
);

INSERT INTO #TEMP_SELLER (id, first_name, last_name, email, gender, country)
SELECT 
    T.X.value('(id)[1]', 'BIGINT') AS id,
    T.X.value('(first_name)[1]', 'VARCHAR(100)') AS first_name,
    T.X.value('(last_name)[1]', 'VARCHAR(100)') AS last_name,
    T.X.value('(email)[1]', 'VARCHAR(MAX') AS email,
    T.X.value('(gender)[1]', 'VARCHAR(30)') AS gender,
    T.X.value('(country)[1]', 'VARCHAR(100)') AS country
FROM @XML_DATA.nodes('dataset/record') AS T(X);

-- Insert only new records into the SELLER table
INSERT INTO SELLER (id, first_name, last_name, email, gender, country)
SELECT 
    id, first_name, last_name, email, gender, country
FROM #TEMP_SELLER AS T
WHERE NOT EXISTS (
    SELECT 1 
    FROM SELLER AS S
    WHERE S.id = T.id
);

-- Clean up
DROP TABLE #TEMP_SELLER;


-------------------------------------------------------------------------------------------------------------------
--------------------JSON CODE FILE----------------------------------
-------------------------------------------------------------------------------------------------------------------
---TWO METHIOD  USE TO READ JSON FILE IN SQL SERVER----------------------------
--1.... DECLARE CODE 
DECLARE @JSON_DATA VARCHAR(MAX);

SELECT @JSON_DATA = BULKCOLUMN
FROM OPENROWSET
(
    BULK 'D:\film\DATA.json', SINGLE_BLOB
) AS DATA_SOURCE;

-- Enable IDENTITY_INSERT for the table
SET IDENTITY_INSERT NEW_TABLE ON;

-- Insert data, including the `id` column
INSERT INTO NEW_TABLE (id, age, salary, name)
SELECT id, age, salary, name
FROM OPENJSON(@JSON_DATA, '$.AAA')
WITH
(
    id INT,
    age INT,
    salary DECIMAL(10,2),
    name VARCHAR(100)
);

-- Disable IDENTITY_INSERT after the operation
SET IDENTITY_INSERT NEW_TABLE OFF;
---- IN JSON FILE ONLY USE ROOT , DO NOT USE THE PATHE       ---- THIS QUERY IS WORKING

------------------------------------------------------------------------------------------
DECLARE @JSON_DATA VARCHAR(MAX)

-- Read JSON data from the file
SELECT @JSON_DATA = BULKCOLUMN
FROM OPENROWSET(
    BULK 'D:\film\DATA.json',
    SINGLE_BLOB
) AS DATA_SOURCE;

-- Create a temporary table to hold JSON data
CREATE TABLE #TEMP_NEW_TABLE (
    id BIGINT, -- This will be ignored during the INSERT
    age INT,
    salary DECIMAL(10, 2),
    name VARCHAR(100)
);


-- Parse JSON into the temporary table
INSERT INTO #TEMP_NEW_TABLE (id, age, salary, name)
SELECT id, age, salary, name
FROM OPENJSON(@JSON_DATA, '$.AAA')
WITH (
    id BIGINT,
    age INT,
    salary DECIMAL(10, 2),
    name VARCHAR(100)
);

-- Insert only new records into NEW_TABLE, including the "id" column
INSERT INTO NEW_TABLE (id,age, salary, name)
SELECT id,age, salary, name
FROM #TEMP_NEW_TABLE AS T
WHERE NOT EXISTS (
    SELECT 1 
    FROM NEW_TABLE AS N
    WHERE N.age = T.age AND N.salary = T.salary AND N.name = T.name
);

-- Clean up
DROP TABLE #TEMP_NEW_TABLE;
------------------------------------------------------------------
--2... DIRECT CODE

SELECT JSON_VALUE(Line, '$.EmployeeID') AS EmployeeID,
       JSON_VALUE(Line, '$.FirstName') AS FirstName,
       JSON_VALUE(Line, '$.LastName') AS LastName
FROM OPENROWSET(BULK 'D:\file\NDJSON_DATA.json', SINGLE_BLOB) AS T(M)
CROSS APPLY OPENJSON(T.M,'$.ROOT') AS Line;       ----  CODE IS CORRECT BUT NOT WORK IN MY MACHINE
---'$.AAA' THIS IS ROOT ,PRESENT IN THE FILE THEN USE OTHERSWISE DO NOT USE

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------


CREATE OR ALTER PROCEDURE new_proc
AS
BEGIN
    BEGIN TRY
        -- Declare variable to hold JSON data
        DECLARE @JSON_DATA VARCHAR(MAX);

        -- Read JSON data from the file
        SELECT @JSON_DATA = BULKCOLUMN
        FROM OPENROWSET(
            BULK 'D:\film\DATA.json', -- Consider passing this path as a parameter
            SINGLE_BLOB
        ) AS DATA_SOURCE;

        -- Create a temporary table to hold JSON data
        CREATE TABLE #TEMP_NEW_TABLE (
            id BIGINT,
            age INT,
            salary DECIMAL(10, 2),
            name VARCHAR(100)
        );

        -- Parse JSON into the temporary table
        INSERT INTO #TEMP_NEW_TABLE (id, age, salary, name)
        SELECT id, age, salary, name
        FROM OPENJSON(@JSON_DATA, '$.AAA')
        WITH (
            id BIGINT,
            age INT,
            salary DECIMAL(10, 2),
            name VARCHAR(100)
        );

        -- Insert only new records into NEW_TABLE, including the "id" column
        INSERT INTO NEW_TABLE (id, age, salary, name)
        SELECT id, age, salary, name
        FROM #TEMP_NEW_TABLE AS T
        WHERE NOT EXISTS (
            SELECT 1 
            FROM NEW_TABLE AS N
            WHERE N.id = T.id -- Use `id` to identify duplicates
        );

        -- Clean up
        DROP TABLE #TEMP_NEW_TABLE;
    END TRY
    BEGIN CATCH
        -- Handle any errors
        PRINT ERROR_MESSAGE();

        -- Ensure the temporary table is dropped even if an error occurs
        IF OBJECT_ID('tempdb..#TEMP_NEW_TABLE') IS NOT NULL
            DROP TABLE #TEMP_NEW_TABLE;
    END CATCH
END;
