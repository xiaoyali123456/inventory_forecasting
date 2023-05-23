# Machine learning solution for inventory forecasting in live ads

## Introduction

* Forecast the inventory for mid-roll advertisements in LIVE sports.
* [PRD](https://docs.google.com/document/d/1_zpanzORHCbvobC9Xb8zRjq9ra4wcyhM1G9xsuCw23w/edit#heading=h.gjdgxs)
* [WIKI](https://hotstar.atlassian.net/wiki/spaces/HP2/pages/3742568242/Live+Inventory+Forecasting)

## Code Hierarchy

```
├── pipeline # code folder in production, others are for experiments
├── data
├── density
├── forecasting
├── sampling
└── readme.md
```
[Pipeline Flow Chart](https://github.com/hotstar/live-ads-inventory-forecasting-ml/blob/main/pipeline/readme.md)

## Storage Path Convention

1. All functions should use CD passed from datapipeline as `current date` for the tasks.
    - Under this rule, the latest source data usually will be on $(CD - 1 Day).
2. Daily data should be stored on exactly the same cd as the original data.
    - For example, watched_video from cd=2023-01-01 should be saved on cd=2023-01-01 in the destination directory.
3. Aggregated data should be stored on the rundate.
