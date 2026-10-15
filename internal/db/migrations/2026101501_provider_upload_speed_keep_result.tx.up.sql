ALTER TABLE provider_upload_speed_tests
  DROP CONSTRAINT IF EXISTS chk_provider_upload_speed_tests_result,
  ADD CONSTRAINT chk_provider_upload_speed_tests_result CHECK (
    (state = 'testing' AND active_task_id IS NOT NULL AND (
      (duration_ms IS NULL AND bytes_per_second IS NULL AND tested_at IS NULL AND failure_code IS NULL)
      OR (duration_ms IS NOT NULL AND duration_ms > 0 AND bytes_per_second IS NOT NULL AND bytes_per_second > 0 AND tested_at IS NOT NULL AND failure_code IS NULL)
      OR (duration_ms IS NULL AND bytes_per_second IS NULL AND tested_at IS NOT NULL AND failure_code IS NOT NULL)
    ))
    OR (state = 'succeeded' AND active_task_id IS NULL AND duration_ms IS NOT NULL AND duration_ms > 0 AND bytes_per_second IS NOT NULL AND bytes_per_second > 0 AND tested_at IS NOT NULL AND failure_code IS NULL)
    OR (state = 'failed' AND active_task_id IS NULL AND duration_ms IS NULL AND bytes_per_second IS NULL AND tested_at IS NOT NULL AND failure_code IS NOT NULL)
  );
