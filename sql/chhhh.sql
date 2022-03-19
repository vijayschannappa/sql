DELIMITER //
DROP PROCEDURE IF EXISTS SP_ETL_RAW_TO_PRO//

CREATE PROCEDURE `SP_ETL_RAW_TO_PRO`(IN from_dt DATETIME,
     IN to_dt DATETIME,IN year VARCHAR(4))
    COMMENT 'STORED PROCEDURE FOR RAW-to-PRO: Averages PSS RAW Attributes over 15 minute time blocks and inserts into PSS PRO TABLE'
BEGIN
    DECLARE from_block DATETIME;
    DECLARE to_block DATETIME;
    DECLARE raw_table TEXT;
    DECLARE pro_table TEXT;

    SET from_block =    UDF_GET_TIME_BLOCK_START('2020-02-04 00:00:00');   -- ex 00:13:03 to 00:00:00
    SET to_block =      UDF_GET_TIME_BLOCK_END('2020-02-04 17:00:00');       -- ex 00:36:13 to 00:45:00
    SET raw_table = CONCAT("DATA_ACTUAL_PSS_RAW_", '2020');
    SET pro_table = CONCAT("DATA_ACTUAL_PSS_PRO_", '2020');
    
    SET @resample_query= CONCAT('INSERT INTO ', @pro_table,  
    '(SUBSTATION_ID, SOURCE_TAG, `TIMESTAMP`, ATTRIBUTE_1, ATTRIBUTE_2, ATTRIBUTE_3, ATTRIBUTE_4, ATTRIBUTE_5)
    (SELECT 
        SUBSTATION_ID, 
        SOURCE_TAG,
        TIME_BLOCK_START AS `TIMESTAMP`,

        AVG(ATTRIBUTE_1) AS ATTRIBUTE_1,
        AVG(ATTRIBUTE_2) AS ATTRIBUTE_2,
        AVG(ATTRIBUTE_3) AS ATTRIBUTE_3,
        AVG(ATTRIBUTE_4) AS ATTRIBUTE_4,
        AVG(ATTRIBUTE_5) AS ATTRIBUTE_5


    FROM 
    (
        SELECT 
            SUBSTATION_ID, SOURCE_TAG,
            UDF_GET_TIME_BLOCK_START(`TIMESTAMP`) as TIME_BLOCK_START,

            ATTRIBUTE_1, ATTRIBUTE_2, ATTRIBUTE_3, ATTRIBUTE_4, ATTRIBUTE_5

        FROM ', raw_table,

        ' WHERE `TIMESTAMP` BETWEEN ','"', from_block, '"', ' AND ','"', to_block,'"',')',
    ' AS DEMAND_RAW',

    ' GROUP BY 
        SUBSTATION_ID, SOURCE_TAG, TIME_BLOCK_START

    HAVING 
        COUNT(*) > 0
    ORDER BY
        SUBSTATION_ID, SOURCE_TAG, TIME_BLOCK_START

    )

    ON DUPLICATE KEY UPDATE
        ATTRIBUTE_1 = VALUES(ATTRIBUTE_1),
        ATTRIBUTE_2 = VALUES(ATTRIBUTE_2),
        ATTRIBUTE_3 = VALUES(ATTRIBUTE_3),
        ATTRIBUTE_4 = VALUES(ATTRIBUTE_4),
        ATTRIBUTE_5 = VALUES(ATTRIBUTE_5)'
    );

END//

DELIMITER ;
