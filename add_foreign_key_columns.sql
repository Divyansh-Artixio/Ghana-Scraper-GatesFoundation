-- Add foreign key columns to public.regulatory_events table
-- This creates proper relationships between regulatory events and companies

BEGIN;

-- Add foreign key columns to regulatory_events table
ALTER TABLE public.regulatory_events 
ADD COLUMN manufacturer_company_id UUID REFERENCES safetydb.companies(id),
ADD COLUMN recalling_firm_company_id UUID REFERENCES safetydb.companies(id),
ADD COLUMN distributor_company_id UUID REFERENCES safetydb.companies(id);

-- Create indexes for better performance
CREATE INDEX idx_regulatory_events_manufacturer_company_id ON public.regulatory_events(manufacturer_company_id);
CREATE INDEX idx_regulatory_events_recalling_firm_company_id ON public.regulatory_events(recalling_firm_company_id);
CREATE INDEX idx_regulatory_events_distributor_company_id ON public.regulatory_events(distributor_company_id);

-- Verify the changes
SELECT 
    column_name, 
    data_type, 
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'public' 
AND table_name = 'regulatory_events'
AND column_name LIKE '%company_id'
ORDER BY column_name;

-- Show the new indexes
SELECT 
    indexname, 
    tablename, 
    indexdef
FROM pg_indexes 
WHERE tablename = 'regulatory_events' 
AND schemaname = 'public'
AND indexname LIKE '%company_id%';

COMMIT;

-- Display success message
SELECT 'Foreign key columns added successfully!' as status;