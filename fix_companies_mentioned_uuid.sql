-- Fix companies_mentioned field to use UUIDs instead of integers
-- This script handles the data type mismatch between:
-- safetydb.companies.id (UUID) and public.regulatory_events.companies_mentioned (Int[])

BEGIN;

-- Step 1: Create a temporary mapping table
DROP TABLE IF EXISTS temp_company_id_mapping;
CREATE TEMP TABLE temp_company_id_mapping (
    old_id INT,
    new_uuid UUID,
    company_name VARCHAR(255)
);

-- Step 2: Map existing companies by name
INSERT INTO temp_company_id_mapping (old_id, new_uuid, company_name)
SELECT 
    pc.id as old_id,
    sc.id as new_uuid,
    pc.name as company_name
FROM public.companies pc
JOIN safetydb.companies sc ON LOWER(TRIM(pc.name)) = LOWER(TRIM(sc.name));

-- Step 3: Add any missing companies to safetydb
INSERT INTO safetydb.companies (name, country_of_origin, established_year, created_at, updated_at)
SELECT 
    pc.name,
    COALESCE(pc.country_code, 'Unknown'),
    EXTRACT(YEAR FROM pc.founding_date),
    pc.created_at,
    pc.updated_at
FROM public.companies pc
LEFT JOIN temp_company_id_mapping tm ON pc.id = tm.old_id
WHERE tm.old_id IS NULL
ON CONFLICT (name) DO NOTHING;

-- Step 4: Update mapping with newly added companies
INSERT INTO temp_company_id_mapping (old_id, new_uuid, company_name)
SELECT 
    pc.id as old_id,
    sc.id as new_uuid,
    pc.name as company_name
FROM public.companies pc
JOIN safetydb.companies sc ON LOWER(TRIM(pc.name)) = LOWER(TRIM(sc.name))
LEFT JOIN temp_company_id_mapping tm ON pc.id = tm.old_id
WHERE tm.old_id IS NULL;

-- Step 5: Create function to convert integer arrays to UUID arrays
CREATE OR REPLACE FUNCTION convert_company_ids_to_uuids(int_array INT[])
RETURNS UUID[] AS $$
DECLARE
    result UUID[] := '{}';
    company_id INT;
    mapped_uuid UUID;
BEGIN
    IF int_array IS NULL OR array_length(int_array, 1) IS NULL THEN
        RETURN result;
    END IF;
    
    FOREACH company_id IN ARRAY int_array
    LOOP
        SELECT new_uuid INTO mapped_uuid 
        FROM temp_company_id_mapping 
        WHERE old_id = company_id;
        
        IF mapped_uuid IS NOT NULL THEN
            result := array_append(result, mapped_uuid);
        END IF;
    END LOOP;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Step 6: Update safetydb.regulatory_events with converted UUIDs
UPDATE safetydb.regulatory_events 
SET companies_mentioned = convert_company_ids_to_uuids(
    (SELECT companies_mentioned FROM public.regulatory_events pre 
     WHERE pre.url = safetydb.regulatory_events.url)
)
WHERE EXISTS (
    SELECT 1 FROM public.regulatory_events pre 
    WHERE pre.url = safetydb.regulatory_events.url 
    AND pre.companies_mentioned IS NOT NULL
);

-- Step 7: Clean up
DROP FUNCTION convert_company_ids_to_uuids(INT[]);

-- Step 8: Verification queries
SELECT 'Company mappings created:' as info, COUNT(*) as count FROM temp_company_id_mapping;

SELECT 'Events with UUID company references:' as info, COUNT(*) as count
FROM safetydb.regulatory_events 
WHERE companies_mentioned IS NOT NULL AND array_length(companies_mentioned, 1) > 0;

-- Sample of converted data
SELECT 
    'Sample converted event' as info,
    url,
    array_length(companies_mentioned, 1) as company_count,
    companies_mentioned[1:2] as sample_uuids
FROM safetydb.regulatory_events 
WHERE companies_mentioned IS NOT NULL 
AND array_length(companies_mentioned, 1) > 0
LIMIT 3;

COMMIT;