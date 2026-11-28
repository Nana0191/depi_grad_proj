import pendulum  # to run the dage with the timezone of the resident 
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator   #bash operator so we can send the date to the python scripts
from airflow.sensors.python import PythonSensor  #this so we can make airflow check on github if it has the data we need with date =ds
from pathlib import Path


default_args = {
    "retries": 3,  #if the task failed it gave it 3 chances
    "retry_delay": timedelta(minutes=1),  #retry after every 5 min 
}

def check_github_batch(batch_date):  #here it will check the githup for the 4 main files with batch date as the intended date
    import requests

    owner = "mennaebied15"
    repo = "olist-batched-data"
    branch = "main"

    expected_files = {
        "customers.csv",
        "orders.csv",
        "order_items.csv",
        "products.csv"
    }

    api_url = (
        f"https://api.github.com/repos/"
        f"{owner}/{repo}/git/trees/{branch}?recursive=1"
    )

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()

    files = response.json()["tree"]

    found_files = {
        Path(item["path"]).name
        for item in files
        if item["type"] == "blob"
        and Path(item["path"]).parent == Path(batch_date)
        and Path(item["path"]).name in expected_files
    }

    return expected_files.issubset(found_files)  #here is the bath ready or still didnot come to the repo



with DAG(  #define the dag
    dag_id="olist_data_pipeline",
    default_args=default_args,
    description="End-to-end Olist data engineering pipeline",
    start_date=pendulum.datetime(2026, 11, 21, tz="Africa/Cairo"),  #here the start date of the dag  creation and accessability
    schedule="0 8 * * *",  #this will run every day at 8 am
    catchup=False,  #means if it had some old runs and dinot make it donot catch up to it 
    tags=["olist", "data-engineering"],
) as dag:
    wait_for_batch = PythonSensor(    #this is  a task that sense the github repo for the data with date = ds
        task_id="wait_for_batch",
        python_callable=check_github_batch,
        op_kwargs={"batch_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}"},
        poke_interval=300,   # it check the repo every 5 min
        timeout=60 * 60 * 1,  #it will gave up if the data didnot reach the repo after 1 hour 
        mode="reschedule",
    )

    gather_data = BashOperator(  #define the first task 
        task_id="gather_data",
        bash_command="python /app/scripts/github_executer.py '{{ data_interval_end.strftime('%Y-%m-%d') }}'",  #this how will airflow run python script 
        #using  this bash command  and ds is the  date when the airflow script runs
    )

    transform_data = BashOperator(  #the second task here 
        task_id="transform_data",
        bash_command=(
            "python /app/scripts/run_pipeline.py "  #the command that will be used to run the transform 
            "--date '{{ data_interval_end.strftime('%Y-%m-%d') }}' " #the first paramater that the transform take 
            "--input-dir /app/data/gathered_data " #the 2nd paramater that the transform take is the place where are the gathered files
            "--out-dir /app/data/processed_data" #3rd param the place or the volume that it will use to store the transformed data
        ),
    )   
    load_data = BashOperator( #here the 3rd task 
        task_id="load_data",
        bash_command=(
            "python /app/scripts/load_to_sqlserver.py "  #how we will run the load script 
            "/app/data/processed_data/runs/{{ data_interval_end.strftime('%Y-%m-%d') }} "
            "--mode incremental" #in the load script i have 2 modes full or incremental
            # i choose incremental bec it will be exhaustive every time to delete the data then full load it so incremental is better
        ),
    ) 
    validate_sql_data = BashOperator(  #this use the sql script validate to check on data every run
        task_id="validate_sql_data",
        bash_command=(
            "/opt/mssql-tools18/bin/sqlcmd "
            "-S sqlserver "
            "-d OlistDW "
            "-U sa "
            "-P \"$OLIST_PASSWORD\" "
            "-C "
            "-b "
            "-i /app/sql/04_validation.sql"
        ),
    )
    wait_for_batch >> gather_data >> transform_data   >> load_data >> validate_sql_data
    #check the repo first if the data is found   it will start to gather it then send it to the transform
    # then the loader takes it and load it in the DB  then apply the validation