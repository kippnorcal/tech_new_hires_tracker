# Tech On-Boarder Provisioning Tracker

## About
At a high level, this connector extracts data from HR's Main On-boarding Tracker (MOT), which lives in a Google Sheet, and loads the data into the Tech On-boarder Provisioning Tracker (Tech Tracker), which is another Google Sheet. 

At a granular level, the connector performs the following:
* A backup copy of the Tech Tracker is created and stored in a Pandas DataFrame
* Existing records in the Tech Tracker are updated with data from MOT
* The backup copy the Tech Tracker is compared to the updated Tech Tracker to identify changes to key pieces of data that the Tech Team needs to be aware of
  * Changes to key pieces of data are tracked by date stamp in a corresponding "Last Updated" column
* The updated Tech Tracker is then compared with the MOT again to identify records in the MOT that are not in the Tech Tracker
  * These records are appended to the Tech Tracker and given a date stamp to indicate the date they were added
* The MOT is checked for rescinded offers to candidates. Rescinded offers are noted in the Tech Tracker
* There is also a "Main Last Updated" column that always reflects the most recent change to key pieces of data. The data set is sorted by this date
* A notification message is sent to KIPP NorCal's notifications Slack channel when job is complete

## Setup
### Dependencies
* Python 3.10
* Docker

### Environment 
The connector needs to be set up with a .env file with the following variables:

``````
# Sheet IDs
TECH_TRACKER_SHEETS_ID=
HR_MOT_SHEETS_ID=

# Google Credentials:
CREDENTIALS_FILE=

# Email notification settings
GMAIL_USER=
GMAIL_PWD=
NOTIF_TO_ADDRESS=
``````

### Google Authentication
The connector will need a .json credential file. The path to this file lives in teh .env file. For more information about setting up Google Authentication, check out the [pygsheets documentation](https://pygsheets.readthedocs.io/en/stable/authorization.html) on it.

## Running the Connector
From the repo's directory, build Docker image
``````
docker build -t tech-tracker-connector .
``````
If using Apple Silicon, use the `--platform` flag
``````
docker build -t tech-tracker-connector . --platform linux/amd64
``````
There is a required runtime argument `--school-year` that accepts year in a XX-XX format.
``````
docker run tech-tracker-connector --school-year 23-24
``````