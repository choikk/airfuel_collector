BEGIN;

ALTER TABLE price_periods
ADD COLUMN IF NOT EXISTS fbo_phone text;

CREATE INDEX IF NOT EXISTS idx_price_periods_fbo_phone
ON price_periods (fbo_phone);

COMMIT;
