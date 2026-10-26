DO $$
BEGIN
  RAISE EXCEPTION 'termination attempt release cannot be rolled back';
END $$;
