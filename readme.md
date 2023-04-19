Machine learning solution for inventory forecasting in live ads

# Storage Path Convention

1. All functions should use CD passed from datapipeline as `current date` for the tasks.
    - Under this rule, the latest source data usually will be on $(CD - 1 Day).
2. Daily data should be stored on exactly the same cd as the original data.
    - For example, watched_video from cd=2023-01-01 should be saved on cd=2023-01-01 in the destination directory.
3. Aggregated data should be stored on the enddate, which is inclusive.
    - For example, data aggregated from [2023-01-01, 2023-03-31] should be stored at cd=2023-03-31.
