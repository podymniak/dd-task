Create a DAG in Airflow with GCP that will:
 1. write 4 files from GCS to BigQuery
 2. Remove duplicated
 3. Create 4 views:
  a. For each region: number of available bikes, number of disabled bikes, number of docks available and number of docks disabled
  b. For each station: station name, average number of trips from the station per day, average number of trips to the station per day.
  c. For each bike number: total trips duration of all trips, total trips count and top subscriber type (subscriber_type column).
  d. The list of top 10 bike numbers with the highest number of trips.

