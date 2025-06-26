SELECT 
  creation_time, 
  job_id, 
  state, 
  user_email, 
  total_slot_ms, 
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), creation_time, SECOND) AS duration_seconds 
FROM 
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT 
WHERE 
  state IN ('RUNNING', 'PENDING') 
  AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) 
ORDER BY 
  creation_time DESC; 
