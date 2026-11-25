-- Run in Databricks SQL. The runner records the actual returned results.
-- baseline
SELECT COUNT(*) AS total_records
FROM workspace.bronze.users_cdc;

-- duplicates
SELECT kafka_topic, kafka_partition, kafka_offset, COUNT(*) AS duplicate_count
FROM workspace.bronze.users_cdc
GROUP BY kafka_topic, kafka_partition, kafka_offset
HAVING COUNT(*) > 1;

-- snapshot
SELECT operation, COUNT(*) AS bronze_records,
       MIN(kafka_offset) AS first_offset, MAX(kafka_offset) AS last_offset
FROM workspace.bronze.users_cdc
GROUP BY operation
ORDER BY CASE operation WHEN 'r' THEN 1 WHEN 'c' THEN 2 WHEN 'u' THEN 3 ELSE 4 END;

-- history (replace :demo_id in SQL Editor, bind it through the connector)
SELECT id, operation, age, kafka_topic, kafka_partition, kafka_offset
FROM workspace.bronze.users_cdc
WHERE id = :demo_id
ORDER BY kafka_partition, kafka_offset;
