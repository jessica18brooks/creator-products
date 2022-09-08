# products
### Description
Uploads CSV to BigQuery table using pandas, creates 2nd table from query results.
Uses a GCP service account to authorise use of BigQuery. 
Regrettably the service account key has been committed to this repo for the sole purpose of others testing this code.
Please [contact me](mailto:jessica18brooks@outlook.com) to gain access to BigQuery to see results.

### How to use
Currently run in cli with the following inputs:
 - Location of text file in local directory `SampleData.csv`

### Future improvements
 - Changing authorisation process to remove service account key from repo
 - Integrate logging into Google Cloud Monitoring
 - Given pandas is such a large library, find a smaller CSV reader if memory is an issue
