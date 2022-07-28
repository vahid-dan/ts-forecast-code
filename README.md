# LAKE-forecast-code
## How to use this template

Suppose you want to use this template in "CIBR" project for a new lake named `test` with the following configurations in `configure_flare.yml`:
  - `forecast met model: noaa/NOAAGEFS_1hr`
  - <200d>`forecast_inflow_model: inflow/FLOWS-NOAAGEFS-AR1<200d><200d>`

For creating a new lake forecast code, follow these steps:

0- Make sure you already have all the necessary drivers for running the forecasts in the S3 storage at:
  - `drivers/noaa/NOAAGEFS_1hr/test`
  - `drivers/inflow/FLOWS-NOAAGEFS-AR1/test`

1- Click the green "Use this template" button on the top right corner of this repository, https://github.com/FLARE-forecast/LAKE-forecast-code.

2- Choose a "Repository name" such as "TEST-forecast-code".

3- Click the green "Create repository from template" button on the bottom of the page.

4- Edit the following files in your newly created repository and change the `site_id` from `fcre` to `test`:
  - `configuration/default/configure_flare.yml`
  - `configuration/default/observation_processing.yml`

5- Edit the following files and change the `forecast_site` from `fcre` to `test`:
  - `main_workflow.R`
  - `workflows/default/combined_workflow.R`

6- Now you can apply your desired changes to the codebase and then run the forecasts.
