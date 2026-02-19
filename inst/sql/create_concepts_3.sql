SELECT DISTINCT t1.unique_id,
            t1.ori_table, 
            t1.person_id,
            t2.concept_id,
            {keep_value_column_name} AS value,
            {keep_date_column_name} AS date{id_set_query}
FROM {cdm_table_name} t1 
INNER JOIN identified_ids t2
 ON t1.unique_id = t2.unique_id
