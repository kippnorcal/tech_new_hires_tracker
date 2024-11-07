# Tech On-Boarder Provisioning Tracker

## About
Querys data from Jobvite for the past 30 days and inserts it into a Google sheet for teh Tech Team to track on-boarders. This job is dependent on HR's MOT for two fields related to employee clearance.

## Setup
### Dependencies
* Python3
* Docker
* See Pipfile for more

### Environment 
The connector needs to be set up with a .env file with the following variables:

``````
# dbt variables
DBT_ACCOUNT_ID=
DBT_JOB_ID=
DBT_BASE_URL=
DBT_PERSONAL_ACCESS_TOKEN=

# Google Storage Info
GOOGLE_APPLICATION_CREDENTIALS=
GBQ_PROJECT=
GBQ_DATASET=

# Sheet IDs
TECH_TRACKER_SHEETS_ID=
HR_MOT_SHEETS_ID=

# Google Credentials:
CREDENTIALS_FILE=

# Email notification settings 
MG_API_KEY=
MG_API_URL=
MG_DOMAIN=
FROM_ADDRESS=
TO_ADDRESS=
``````

### Google Authentication
The connector will need a .json credential file. The path to this file lives in teh .env file. For more information about setting up Google Authentication, check out the [pygsheets documentation](https://pygsheets.readthedocs.io/en/stable/authorization.html) on it.

## Running the Connector
From the repo's directory, build Docker image
``````
docker build -t tech-tracker-connector .
``````

There is a required runtime argument `--school-year` that accepts year in a YY-YY format.
``````
docker run tech-tracker-connector --school-year 24-25
``````