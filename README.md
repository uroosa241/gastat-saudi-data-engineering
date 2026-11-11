# gastat-saudi-data-engineering

End-to-end Azure data engineering project for GASTAT (Saudi General Authority for Statistics) national accounts data (GDP by expenditure, GDP by income, GDP by ISIC activity).

* Azure Data Factory ingests the GASTAT API (https://api.stats.gov.sa) into ADLS Gen2 (`stgastsaudi12`: raw / bronze containers).
* Databricks notebooks build the silver layer (cleaned, typed, deduplicated, DQ-audited) and the gold layer (analytics-ready tables) in Unity Catalog.
* Databricks resources are deployed with a Declarative Automation Bundle; ADF is deployed as an ARM template. Both via GitHub Actions.

## Folder structure

```
.
├── .github/workflows/
│   ├── databricks-cicd.yml      # validate on PR, deploy dev -> prod on merge to main
│   └── adf-cicd.yml             # build/export ARM template, deploy to prod ADF
├── adf/                         # ADF Git-integrated factory (pipeline, dataset, linkedService, factory)
│   └── build/package.json       # @microsoft/azure-data-factory-utilities build
├── databricks/
│   ├── databricks.yml           # bundle: gastat_pipeline (targets dev, prod)
│   ├── resources/
│   │   └── gastat_pipeline.job.yml   # job gastat_silver_gold: silver -> gold
│   ├── notebooks/
│   │   ├── silver/gastat_silver_build
│   │   └── gold/gastat_gold_build
│   └── config/pipelines/gdp_income.json
├── tests/test_transforms.py
└── requirements-dev.txt
```

## Bundle variables

| Variable | dev | prod |
| --- | --- | --- |
| catalog | gastatksa_dev | gastatksa |
| silver_schema | silver | silver |
| gold_schema | gold | gold |

These are passed to the notebooks as the widgets `catalog`, `silver_schema`, `gold_schema`.

## How to deploy

1. Create a feature branch from `main` (e.g. `feature/<short-description>`).
2. Make changes and push the branch.
3. Open a pull request into `main`. CI runs:
   * Databricks: `pytest` and `databricks bundle validate -t dev` (when `databricks/**` changes).
   * ADF: validate and export the ARM template (when `adf/**` changes).
4. After review, merge the pull request.
5. On push to `main`:
   * Databricks: `databricks bundle deploy -t dev`, then (after approval on the `prod` environment) `databricks bundle deploy -t prod`.
   * ADF: stop triggers, deploy the ARM template (Incremental) to the prod factory, restart triggers.

### Local deploy (optional)

```
cd databricks
export DATABRICKS_HOST=https://<workspace-host>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev gastat_silver_gold
```

## Required GitHub secrets

`DATABRICKS_HOST_DEV`, `DATABRICKS_HOST_PROD`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID` (configure in GitHub environments `dev` and `prod`). No secrets or tokens are stored in this repository.
