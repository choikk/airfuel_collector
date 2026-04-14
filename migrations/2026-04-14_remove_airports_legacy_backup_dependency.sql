BEGIN;

-- Backfill site_no directly from airports_v2 when airport_code already matches.
UPDATE price_periods AS p
SET site_no = a.site_no
FROM airports_v2 AS a
WHERE p.airport_code = a.airport_code
  AND p.site_no IS NULL
  AND a.site_no IS NOT NULL;

-- Rewrite old airport codes through airport_code_map and sync site_no.
UPDATE price_periods AS p
SET airport_code = a.airport_code,
    site_no = a.site_no
FROM airport_code_map AS m
JOIN airports_v2 AS a
  ON a.airport_code = m.new_airport_code
WHERE p.airport_code = m.old_airport_code
  AND a.site_no IS NOT NULL
  AND (
        p.airport_code IS DISTINCT FROM a.airport_code
        OR p.site_no IS DISTINCT FROM a.site_no
      );

-- Canonicalize airport_code for rows that already have site_no.
UPDATE price_periods AS p
SET airport_code = a.airport_code
FROM airports_v2 AS a
WHERE p.site_no IS NOT NULL
  AND p.site_no = a.site_no
  AND p.airport_code IS DISTINCT FROM a.airport_code;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM price_periods AS p
        LEFT JOIN airports_v2 AS a
          ON a.airport_code = p.airport_code
        WHERE a.airport_code IS NULL
    ) THEN
        RAISE EXCEPTION
            'price_periods contains airport_code values that do not exist in airports_v2';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM price_periods
        WHERE site_no IS NULL
    ) THEN
        RAISE EXCEPTION
            'price_periods still contains rows with NULL site_no after backfill';
    END IF;
END $$;

ALTER TABLE price_periods
DROP CONSTRAINT IF EXISTS price_periods_airport_code_fkey;

ALTER TABLE price_periods
ADD CONSTRAINT price_periods_airport_code_fkey
FOREIGN KEY (airport_code)
REFERENCES airports_v2(airport_code)
ON DELETE CASCADE
ON UPDATE CASCADE;

DROP INDEX IF EXISTS uniq_price_periods_open;

CREATE UNIQUE INDEX uniq_price_periods_open
ON price_periods (site_no, fbo_name, fuel_type, service_type)
WHERE valid_to IS NULL;

COMMIT;
