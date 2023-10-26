SELECT DISTINCT
    c.job_eid + '-' +c.candidate_eid AS job_candidate_id
    ,c.firstName AS first_name
    ,c.lastName AS last_name
    ,c.is_this_a_rehire_promotion_or_internal_transfer as hire_reason
    ,c.email AS personal_email
    ,c.assigned_work_location AS work_location
    ,c.assigned_pay_location AS pay_location
    ,c.startDate AS start_date
    ,c.title AS title
    ,c.formerOrCurrentKIPP AS "former_or_current_kipp"
    --SpEd? column will be generated here by Python
    , c.disposition
    ,c.lastUpdatedDate AS last_updated_date
FROM custom.jobvite_full c
WHERE lastUpdatedDate >= DATEADD(MONTH, -1, GETDATE())
    AND lastUpdatedDate < GETDATE()
    AND workflowState = 'Offer Accepted'
ORDER BY lastUpdatedDate DESC