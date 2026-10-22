DO $$
BEGIN
  RAISE EXCEPTION 'storage removal batches cannot be rolled back';
END $$;
