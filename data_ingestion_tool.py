import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

# Arguments
default_args = {
    'owner': 'MP',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# DAG
dag = DAG('data_ingestion_tool',
           schedule_interval=None,
           dagrun_timeout=timedelta(minutes=10),
           default_args=default_args
)

# Start
start = DummyOperator(task_id='start_dag',dag = dag)

# Create datasets
def create_dataset(ds_name):
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id=f"check-dataset_{ds_name}",
        project_id = 'delvedeepertask',
        dataset_id=ds_name,
        location='europe-central2',
        exists_ok=True, # create if doesn't exist
        dag = dag)
    return create_dataset

c1 = create_dataset('stage')
c2 = create_dataset('views')

# Mid
mid = DummyOperator(task_id='start_data_load',dag = dag) # dummy to group tasks

################################################# Data load
def load_files(filename,filetype):
    if filetype == 'csv': source_format='CSV'; skip_leading_rows=1
    else: source_format='NEWLINE_DELIMITED_JSON'; skip_leading_rows=0
    # part={'type':"DAY"} if 'trips' in filename else None
    write_disposition='WRITE_APPEND' if 'trips' in filename else 'WRITE_TRUNCATE'
    
    task = GoogleCloudStorageToBigQueryOperator(
        task_id=f'data_load_{filename.split("-")[0]}',
        bucket= 'europe-central2-dd-environm-dcc7285d-bucket',
        source_objects = [f'stored_files/{filename}.{filetype}'],
        source_format=source_format,
        # time_partitioning=part,
        skip_leading_rows=skip_leading_rows,
        destination_project_dataset_table=f'delvedeepertask.stage.{filename.split("-")[0]}', # without date for trips table
        write_disposition=write_disposition,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id='bigquery_default',
        dag = dag
    )
    return task

t1 = load_files('regions','csv') 
t2 = load_files('trips-*','csv')
t3 = load_files('station_info','json') 
t4 = load_files('station_status','json') 

################################################# Remove duplicates
sql = '''
SELECT * EXCEPT(row_number) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY cast(trip_id as string)) row_number FROM `delvedeepertask.stage.trips`)
WHERE row_number = 1
'''

clean = BigQueryOperator(
    sql = sql,
    task_id='clean_trips_table',
    destination_dataset_table='delvedeepertask.stage.trips',
    write_disposition='WRITE_TRUNCATE',
    location='europe-central2',
    bigquery_conn_id='bigquery_default',
    use_legacy_sql=False,
    dag = dag
)

################################################# Create views
sql = {
       'region_stats':
            '''
            SELECT r.region_id, r.name, sum(num_bikes_available) num_bikes_available, sum(num_bikes_disabled) num_bikes_disabled,
            sum(num_docks_available) num_docks_available, sum(num_docks_disabled) num_docks_disabled            
            FROM `delvedeepertask.stage.station_status` ss
            left join `delvedeepertask.stage.station_info` si on ss.station_id=si.station_id
            left join `delvedeepertask.stage.regions` r on si.region_id=r.region_id
            group by r.region_id, r.name
            order by 1
            ''',
        'station_stats':
            '''
            select start_station_id station_id, start_station_name station_name, trips_from_per_day, trips_to_per_day from (
            SELECT start_station_id, start_station_name, round(count(trip_id)/count(distinct date(start_date)),1) as trips_from_per_day
            FROM `delvedeepertask.stage.trips` 
            group by start_station_id, start_station_name)
            join (
            SELECT end_station_id, end_station_name, round(count(trip_id)/count(distinct date(end_date)),1) as trips_to_per_day
            FROM `delvedeepertask.stage.trips` 
            group by end_station_id, end_station_name)
            on start_station_id=end_station_id
            order by 1
            ''',
        'top_10_bikes':
            '''
            SELECT bike_number, count(trip_id) num_of_trips FROM `delvedeepertask.stage.trips` 
            group by bike_number
            order by 2 desc limit 10
            ''',
        'bike_stats':
            '''
            with tbl1 as 
                (select a.bike_number,a.subscriber_type,count(a.subscriber_type) as val_count 
                from `delvedeepertask.stage.trips` a
                group by a.bike_number,a.subscriber_type)
            select t1.bike_number,sum(t3.duration_sec) sum_duration_sec, count(t3.trip_id) num_of_trips,t1.subscriber_type as top_subscriber_type from tbl1 t1
            inner join 
                (select bike_number,max(val_count) as val_count from tbl1 group by bike_number) t2
                on t1.bike_number=t2.bike_number and t1.val_count=t2.val_count
            join `delvedeepertask.stage.trips` t3 on t1.bike_number=t3.bike_number
            group by t1.bike_number,t1.subscriber_type
            order by 1
            '''
        }

def create_view(view_name):
    view = BigQueryOperator(
        sql = sql[view_name],
        task_id=f'create_view_{view_name}',
        destination_dataset_table=f'delvedeepertask.views.{view_name}',
        write_disposition='WRITE_TRUNCATE',
        location='europe-central2',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        create_disposition='CREATE_IF_NEEDED',
        dag = dag
    )
    return view

v1 = create_view('region_stats')
v2 = create_view('station_stats')
v3 = create_view('top_10_bikes')
v4 = create_view('bike_stats')

# Dependencies
start >> (c1,c2) >> mid >> (t1, t2, t3, t4) >> clean >> (v1, v2, v3, v4)











































