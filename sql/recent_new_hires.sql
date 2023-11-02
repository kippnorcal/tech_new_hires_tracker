SELECT DISTINCT
    c.job_eid + '-' +c.candidate_eid AS job_candidate_id
    ,c.firstName AS "First Name"
    ,c.lastName AS "Last Name"
    ,c.is_this_a_rehire_promotion_or_internal_transfer as "New, Returners, Rehire or Transfer"
    ,c.email AS "Personal Email"
    ,c.assigned_work_location AS "Work Location"
    ,c.assigned_pay_location AS "Pay Location"
    ,c.startDate AS "Start Date"
    ,c.title AS "Title"
    ,c.formerOrCurrentKIPP AS "Former or Current KIPP"
FROM custom.jobvite_full c
WHERE lastUpdatedDate >= DATEADD(MONTH, -1, GETDATE())
    AND lastUpdatedDate < GETDATE()
    AND workflowState = 'Offer Accepted'