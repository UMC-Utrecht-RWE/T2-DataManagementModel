DROP TABLE IF EXISTS identified_ids;

CREATE TABLE IF NOT EXISTS identified_ids AS
    SELECT t.unique_id, c.id_set, c.concept_id, c.order_index
    FROM {cdm_table_name} t
    INNER JOIN codelist c
    ON t.{cdm_column} = c.code
    LIMIT 0;

INSERT INTO identified_ids
    SELECT t.unique_id, c.id_set, c.concept_id, c.order_index
    FROM {cdm_table_name} t
    INNER JOIN codelist c
    ON t.{cdm_column} = c.code;
