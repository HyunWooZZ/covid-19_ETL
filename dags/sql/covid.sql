CREATE OR REPLACE TABLE
    `{{ params.dwh_dataset }}.data_mart` 
AS SELECT 
    iso_code AS ISO_CODE,
    continent AS Continent,
    location AS Country,
    total_cases,
    new_cases,
    total_deaths,
    new_deaths,
    total_cases_per_million,
    new_cases_per_million,
    total_deaths_per_million,
    new_deaths_per_million,
    reproduction_rate,
    icu_patients,
    icu_patients_per_million,
    hosp_patients,
    hosp_patients_per_million,
    weekly_icu_admissions,
    total_tests_per_thousand,
    new_tests_per_thousand,
    total_vaccinations,
    new_vaccinations
FROM 
    `{{ params.project_id }}.{{ params.staging_dataset }}.covid_data`;



