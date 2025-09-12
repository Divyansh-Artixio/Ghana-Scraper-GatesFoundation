-- Ghana FDA Regulatory Scraper - Company-Product Recall Relationship Queries

-- =====================================================
-- 1. BASIC COMPANY-PRODUCT RECALL RELATIONSHIPS
-- =====================================================

-- Show all product recalls with their linked companies
SELECT 
    re.id,
    re.product_name,
    re.recall_date,
    m.name as manufacturer,
    rf.name as recalling_firm,
    re.reason_for_action
FROM regulatory_events re
LEFT JOIN companies m ON re.manufacturer_id = m.id
LEFT JOIN companies rf ON re.recalling_firm_id = rf.id
WHERE re.event_type = 'Product Recall'
ORDER BY re.recall_date DESC;

-- =====================================================
-- 2. COMPANY STATISTICS
-- =====================================================

-- Top manufacturers by number of recalls
SELECT 
    m.name as manufacturer,
    m.type,
    m.country_code,
    COUNT(*) as recall_count
FROM regulatory_events re
JOIN companies m ON re.manufacturer_id = m.id
WHERE re.event_type = 'Product Recall'
GROUP BY m.id, m.name, m.type, m.country_code
ORDER BY recall_count DESC;

-- Top recalling firms by number of recalls
SELECT 
    rf.name as recalling_firm,
    rf.type,
    rf.country_code,
    COUNT(*) as recall_count
FROM regulatory_events re
JOIN companies rf ON re.recalling_firm_id = rf.id
WHERE re.event_type = 'Product Recall'
GROUP BY rf.id, rf.name, rf.type, rf.country_code
ORDER BY recall_count DESC;

-- =====================================================
-- 3. DETAILED COMPANY-PRODUCT ANALYSIS
-- =====================================================

-- Products recalled by specific manufacturer (example: Atlantic Life Science Ltd)
SELECT 
    re.product_name,
    re.product_type,
    re.recall_date,
    re.batches,
    re.reason_for_action,
    rf.name as recalling_firm
FROM regulatory_events re
JOIN companies m ON re.manufacturer_id = m.id
LEFT JOIN companies rf ON re.recalling_firm_id = rf.id
WHERE re.event_type = 'Product Recall' 
  AND m.name = 'Atlantic Life Science Ltd'
ORDER BY re.recall_date DESC;

-- Companies that are both manufacturers and recalling firms
SELECT DISTINCT
    c.name,
    c.type,
    c.country_code,
    COUNT(CASE WHEN re1.manufacturer_id = c.id THEN 1 END) as as_manufacturer,
    COUNT(CASE WHEN re2.recalling_firm_id = c.id THEN 1 END) as as_recalling_firm
FROM companies c
LEFT JOIN regulatory_events re1 ON re1.manufacturer_id = c.id AND re1.event_type = 'Product Recall'
LEFT JOIN regulatory_events re2 ON re2.recalling_firm_id = c.id AND re2.event_type = 'Product Recall'
GROUP BY c.id, c.name, c.type, c.country_code
HAVING COUNT(CASE WHEN re1.manufacturer_id = c.id THEN 1 END) > 0 
   AND COUNT(CASE WHEN re2.recalling_firm_id = c.id THEN 1 END) > 0
ORDER BY c.name;

-- =====================================================
-- 4. PRODUCT TYPE ANALYSIS BY COMPANY
-- =====================================================

-- Product types by manufacturer
SELECT 
    m.name as manufacturer,
    re.product_type,
    COUNT(*) as product_count
FROM regulatory_events re
JOIN companies m ON re.manufacturer_id = m.id
WHERE re.event_type = 'Product Recall' AND re.product_type IS NOT NULL
GROUP BY m.name, re.product_type
ORDER BY m.name, product_count DESC;

-- =====================================================
-- 5. TIMELINE ANALYSIS
-- =====================================================

-- Company recall activity by month
SELECT 
    DATE_TRUNC('month', re.recall_date) as recall_month,
    m.name as manufacturer,
    COUNT(*) as recalls_in_month
FROM regulatory_events re
JOIN companies m ON re.manufacturer_id = m.id
WHERE re.event_type = 'Product Recall' AND re.recall_date IS NOT NULL
GROUP BY DATE_TRUNC('month', re.recall_date), m.name
ORDER BY recall_month DESC, recalls_in_month DESC;

-- =====================================================
-- 6. COMPANY RELATIONSHIP MAPPING
-- =====================================================

-- Find cases where manufacturer and recalling firm are different
SELECT 
    re.product_name,
    re.recall_date,
    m.name as manufacturer,
    rf.name as recalling_firm,
    CASE 
        WHEN m.id = rf.id THEN 'Same Company'
        ELSE 'Different Companies'
    END as relationship
FROM regulatory_events re
JOIN companies m ON re.manufacturer_id = m.id
JOIN companies rf ON re.recalling_firm_id = rf.id
WHERE re.event_type = 'Product Recall'
ORDER BY re.recall_date DESC;

-- =====================================================
-- 7. MISSING COMPANY LINKS
-- =====================================================

-- Product recalls without manufacturer information
SELECT 
    re.id,
    re.product_name,
    re.recall_date,
    re.manufacturer_id,
    re.recalling_firm_id
FROM regulatory_events re
WHERE re.event_type = 'Product Recall' 
  AND (re.manufacturer_id IS NULL OR re.recalling_firm_id IS NULL)
ORDER BY re.recall_date DESC;

-- =====================================================
-- 8. COMPREHENSIVE COMPANY PROFILE
-- =====================================================

-- Complete profile for a specific company (replace 'Atlantic Life Science Ltd' with any company name)
WITH company_profile AS (
    SELECT id, name, type, country_code FROM companies WHERE name = 'Atlantic Life Science Ltd'
)
SELECT 
    'As Manufacturer' as role,
    re.product_name,
    re.product_type,
    re.recall_date,
    re.reason_for_action,
    rf.name as recalling_firm
FROM company_profile cp
JOIN regulatory_events re ON re.manufacturer_id = cp.id
LEFT JOIN companies rf ON re.recalling_firm_id = rf.id
WHERE re.event_type = 'Product Recall'

UNION ALL

SELECT 
    'As Recalling Firm' as role,
    re.product_name,
    re.product_type,
    re.recall_date,
    re.reason_for_action,
    m.name as manufacturer
FROM company_profile cp
JOIN regulatory_events re ON re.recalling_firm_id = cp.id
LEFT JOIN companies m ON re.manufacturer_id = m.id
WHERE re.event_type = 'Product Recall'
ORDER BY recall_date DESC;

-- =====================================================
-- 9. EXPORT-READY QUERY
-- =====================================================

-- Complete dataset for export (use this for CSV export)
SELECT 
    re.id as recall_id,
    re.product_name,
    re.product_type,
    re.recall_date,
    re.batches,
    re.manufacturing_date,
    re.expiry_date,
    re.reason_for_action,
    
    -- Manufacturer information
    re.manufacturer_id,
    m.name as manufacturer_name,
    m.type as manufacturer_type,
    m.country_code as manufacturer_country,
    
    -- Recalling firm information
    re.recalling_firm_id,
    rf.name as recalling_firm_name,
    rf.type as recalling_firm_type,
    rf.country_code as recalling_firm_country,
    
    -- Additional fields
    re.source_url,
    re.pdf_path,
    re.created_at
    
FROM regulatory_events re
LEFT JOIN companies m ON re.manufacturer_id = m.id
LEFT JOIN companies rf ON re.recalling_firm_id = rf.id
WHERE re.event_type = 'Product Recall'
ORDER BY re.recall_date DESC, re.id;

-- =====================================================
-- 10. QUICK SUMMARY STATISTICS
-- =====================================================

-- Overall summary
SELECT 
    'Total Product Recalls' as metric,
    COUNT(*) as value
FROM regulatory_events 
WHERE event_type = 'Product Recall'

UNION ALL

SELECT 
    'Recalls with Manufacturer Info' as metric,
    COUNT(*) as value
FROM regulatory_events 
WHERE event_type = 'Product Recall' AND manufacturer_id IS NOT NULL

UNION ALL

SELECT 
    'Recalls with Recalling Firm Info' as metric,
    COUNT(*) as value
FROM regulatory_events 
WHERE event_type = 'Product Recall' AND recalling_firm_id IS NOT NULL

UNION ALL

SELECT 
    'Unique Manufacturers' as metric,
    COUNT(DISTINCT manufacturer_id) as value
FROM regulatory_events 
WHERE event_type = 'Product Recall' AND manufacturer_id IS NOT NULL

UNION ALL

SELECT 
    'Unique Recalling Firms' as metric,
    COUNT(DISTINCT recalling_firm_id) as value
FROM regulatory_events 
WHERE event_type = 'Product Recall' AND recalling_firm_id IS NOT NULL;