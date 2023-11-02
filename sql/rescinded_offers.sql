SELECT DISTINCT
    c.job_eid + '-' +c.candidate_eid AS job_candidate_id
    , c.firstName + ' ' + c.lastName AS name
    , c.lastUpdatedDate
    , c.disposition
FROM custom.jobvite_full c
WHERE lastUpdatedDate >= DATEADD(MONTH, -1, GETDATE())
    AND lastUpdatedDate < GETDATE()
    AND workflowState = 'Offer Accepted'
    AND disposition not in ('Hired', '')
ORDER BY lastUpdatedDate DESC
