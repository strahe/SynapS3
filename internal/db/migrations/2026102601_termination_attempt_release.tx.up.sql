ALTER TABLE storage_data_set_termination_attempts ADD COLUMN IF NOT EXISTS unsent_at timestamptz;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'storage_data_set_termination_attempts'::regclass
      AND conname = 'chk_storage_data_set_termination_attempts_unsent'
  ) THEN
    ALTER TABLE storage_data_set_termination_attempts ADD CONSTRAINT chk_storage_data_set_termination_attempts_unsent
      CHECK (unsent_at IS NULL OR (tx_hash IS NULL AND confirmed_tx_hash IS NULL AND receipt_status IS NULL AND task_id IS NOT NULL));
  END IF;
END $$;
