
WITH final_records AS( -- 1. Expand information of the identified_ids
    SELECT DISTINCT t1.unique_id, 
                t1.ori_table, 
                t1.ROWID, 
                t1.person_id,
                {keep_value_column_name} AS value, 
                t2.concept_id,
                {keep_date_column_name} AS date, 
    FROM {cdm_table_name} t1 
    INNER JOIN identified_ids t2
     ON t1.unique_id = t2.unique_id
)

INSERT INTO concept_table 
SELECT *
FROM final_records
ON CONFLICT DO UPDATE SET
    unique_id = EXCLUDED.unique_id,
    concept_id = EXCLUDED.concept_id;

DROP TABLE identified_ids;
