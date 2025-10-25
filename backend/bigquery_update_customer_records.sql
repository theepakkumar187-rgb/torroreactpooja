-- BigQuery Update Commands for torrodataset.banking_pii.customer_records
-- Generated on 2025-10-24 05:33:41


-- Update column description for customer_id
ALTER TABLE `torrodataset.banking_pii.customer_records`
ALTER COLUMN customer_id SET OPTIONS (description = 'Tags: PII, IDENTIFIER');


-- Update column description for ssn
ALTER TABLE `torrodataset.banking_pii.customer_records`
ALTER COLUMN ssn SET OPTIONS (description = 'Tags: SENSITIVE, PII');


-- Update table labels
ALTER TABLE `torrodataset.banking_pii.customer_records`
SET OPTIONS (
  labels = [tag_0 = 'pii', tag_1 = 'identifier', tag_2 = 'sensitive']
);

