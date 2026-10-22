CREATE TABLE IF NOT EXISTS storage_removal_requests (
  request_id text PRIMARY KEY,
  storage_data_set_id bigint NOT NULL,
  status text NOT NULL,
  task_id bigint,
  sealed_at timestamptz,
  queued_at timestamptz,
  queued_block bigint,
  attention_code text,
  attention_at timestamptz,
  last_error text,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT uq_storage_removal_requests_data_set UNIQUE (request_id, storage_data_set_id),
  CONSTRAINT chk_storage_removal_requests_identity CHECK (request_id <> '' AND (attention_code IS NULL OR attention_code <> '') AND (last_error IS NULL OR last_error <> '')),
  CONSTRAINT chk_storage_removal_requests_status CHECK (status IN ('collecting', 'ready', 'submitted', 'queued', 'settled', 'abandoned')),
  CONSTRAINT chk_storage_removal_requests_task CHECK ((status IN ('settled', 'abandoned')) = (task_id IS NULL)),
  CONSTRAINT chk_storage_removal_requests_attention CHECK ((attention_code IS NULL) = (attention_at IS NULL) AND (attention_code IS NULL OR status IN ('ready', 'submitted', 'queued'))),
  CONSTRAINT chk_storage_removal_requests_queue CHECK ((queued_at IS NULL) = (queued_block IS NULL) AND (queued_block IS NULL OR queued_block >= 0)),
  CONSTRAINT chk_storage_removal_requests_shape CHECK (
    (status = 'collecting' AND sealed_at IS NULL AND queued_at IS NULL) OR
    (status IN ('ready', 'submitted') AND sealed_at IS NOT NULL AND queued_at IS NULL) OR
    (status IN ('queued', 'settled') AND sealed_at IS NOT NULL AND queued_at IS NOT NULL) OR
    (status = 'abandoned' AND queued_at IS NULL AND last_error IS NOT NULL)
  ),
  FOREIGN KEY (storage_data_set_id) REFERENCES storage_data_sets (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_storage_removal_requests_collecting ON storage_removal_requests (storage_data_set_id) WHERE status = 'collecting';
CREATE UNIQUE INDEX IF NOT EXISTS uq_storage_removal_requests_task ON storage_removal_requests (task_id) WHERE task_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_storage_removal_requests_data_set ON storage_removal_requests (storage_data_set_id, status, created_at, request_id);
CREATE INDEX IF NOT EXISTS idx_storage_removal_requests_attention ON storage_removal_requests (attention_at, request_id) WHERE attention_code IS NOT NULL;

--bun:split

CREATE TABLE IF NOT EXISTS storage_removal_sends (
  request_id text NOT NULL,
  send_no integer NOT NULL,
  task_id bigint,
  attempted_at timestamptz,
  tx_hash text,
  confirmed_tx_hash text,
  receipt_status smallint,
  refused_at timestamptz,
  unsent_at timestamptz,
  last_error text,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (request_id, send_no),
  CONSTRAINT chk_storage_removal_sends_number CHECK (send_no > 0),
  CONSTRAINT chk_storage_removal_sends_identity CHECK (request_id <> '' AND (tx_hash IS NULL OR tx_hash <> '') AND (confirmed_tx_hash IS NULL OR confirmed_tx_hash <> '') AND (last_error IS NULL OR last_error <> '')),
  CONSTRAINT chk_storage_removal_sends_receipt CHECK (receipt_status IS NULL OR (receipt_status IN (0, 1) AND confirmed_tx_hash IS NOT NULL)),
  CONSTRAINT chk_storage_removal_sends_refusal CHECK ((refused_at IS NULL OR unsent_at IS NULL) AND ((refused_at IS NULL AND unsent_at IS NULL) OR (tx_hash IS NULL AND confirmed_tx_hash IS NULL AND receipt_status IS NULL))),
  CONSTRAINT chk_storage_removal_sends_evidence CHECK (tx_hash IS NOT NULL OR (task_id IS NOT NULL AND attempted_at IS NOT NULL)),
  FOREIGN KEY (request_id) REFERENCES storage_removal_requests (request_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_storage_removal_sends_task ON storage_removal_sends (task_id) WHERE task_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS storage_removal_send_pieces (
  request_id text NOT NULL,
  send_no integer NOT NULL,
  cleanup_copy_id bigint NOT NULL,
  piece_id text NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (request_id, send_no, cleanup_copy_id),
  CONSTRAINT chk_storage_removal_send_pieces_identity CHECK (request_id <> '' AND piece_id <> ''),
  FOREIGN KEY (request_id, send_no) REFERENCES storage_removal_sends (request_id, send_no) ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (cleanup_copy_id) REFERENCES storage_cleanup_copies (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_storage_removal_send_pieces_copy ON storage_removal_send_pieces (cleanup_copy_id);

--bun:split

ALTER TABLE storage_cleanup_copies ADD COLUMN IF NOT EXISTS removal_request_id text;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'storage_cleanup_copies'::regclass AND conname = 'fk_storage_cleanup_copies_removal') THEN
    ALTER TABLE storage_cleanup_copies ADD CONSTRAINT fk_storage_cleanup_copies_removal FOREIGN KEY (removal_request_id, storage_data_set_id) REFERENCES storage_removal_requests (request_id, storage_data_set_id) ON UPDATE RESTRICT ON DELETE RESTRICT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'storage_cleanup_copies'::regclass AND conname = 'chk_storage_cleanup_copies_removal') THEN
    ALTER TABLE storage_cleanup_copies ADD CONSTRAINT chk_storage_cleanup_copies_removal CHECK (removal_request_id IS NULL OR (removal_request_id <> '' AND status IN ('pending', 'delete_scheduled', 'removed')));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_storage_cleanup_copies_removal ON storage_cleanup_copies (removal_request_id);
