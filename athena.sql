SELECT * FROM medicare_provider LIMIT 10;

SELECT bus_eff_date, COUNT(*) as no_records
FROM medicare_provider
GROUP BY bus_eff_date;

SELECT * FROM medicare_provider
WHERE bus_eff_date = '2018-06-03'
AND operation = 'I';

SELECT * FROM medicare_provider
WHERE bus_eff_date = '2018-06-03'
AND operation = 'D';

SELECT * FROM medicare_provider
WHERE bus_eff_date = '2018-06-03'
AND operation = 'U';

CREATE OR REPLACE VIEW medicare_provider_current AS
SELECT 
  drg, 
  provider_id, 
  provider_name, 
  provider_street_address, 
  provider_city, 
  provider_state, 
  provider_zip, 
  rr, 
  total_discharges, 
  charges_covered, 
  charges_total_pay, 
  charges_medicare_pay,
  bus_eff_date
FROM medicare_provider
WHERE bus_eff_date = '2018-06-03'
AND eff_end_date IS NULL;

SELECT * FROM medicare_provider_current LIMIT 10;

--
--
--
SELECT * FROM medicare_provider_current LIMIT 10;

SELECT * FROM medicare_provider LIMIT 10;

SELECT bus_eff_date, COUNT(*) as no_records
FROM medicare_provider
GROUP BY bus_eff_date;

SELECT * FROM medicare_provider
WHERE bus_eff_date = '2018-06-03'
AND operation = 'I';

SELECT * FROM medicare_provider
WHERE bus_eff_date = '2018-06-03'
AND operation = 'D';

SELECT * FROM medicare_provider
WHERE bus_eff_date = '2018-06-03'
AND operation = 'U';