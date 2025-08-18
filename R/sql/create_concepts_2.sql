WITH identified_n_minus_1 AS (-- 1. Get previous ids matched at order_index - 1
  SELECT ii.*
  FROM identified_ids ii
  INNER JOIN codelist cn
  ON ii.order_index = cn.order_index - 1 AND ii.id_set = cn.id_set
)
-- 2. Collect the column needed for further selection
,identified_records AS (
    SELECT t.ori_id, t.{child_col}, in1.id_set
    FROM {cdm_table_name} t
    INNER JOIN identified_n_minus_1 in1
    ON t.ori_id = in1.ori_id
)
-- 3. Find those that don't match current codes
,to_delete AS (
  SELECT t.ori_id, t.{child_col}, t.id_set
  FROM identified_records t
  LEFT OUTER JOIN codelist c
  ON t.{child_col} = c.code AND t.id_set = c.id_set
  WHERE c.code IS NULL
)
-- 4. Delete from the main id table
DELETE FROM identified_ids
WHERE (ori_id, id_set) IN (
  SELECT ori_id, id_set
  FROM to_delete
);
