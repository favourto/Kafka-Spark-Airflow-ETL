### How to run code
1. create virtual environment 
- python3 -m venv wiki
- source wiki/bin/activate
- pip install -r requirements.txt
- Run the docker compose up to spin up kafka cluster 
    - docker compose up

### Download postgres and go into postgres database and create table and add indexing
- psql -U postgres
- CREATE ROLE you WITH LOGIN PASSWORD 'you.com';
- CREATE TABLE tz_table (
     referrer VARCHAR,
     title VARCHAR,
     count INT,
     date_created DATE,
     type VARCHAR);
- CREATE INDEX idx_tz_date_created ON tz_table (date_created);
- CREATE INDEX idx_tz_title ON tz_table (title);
- CREATE INDEX idx_tz_referrer ON tz_table (referrer);
- GRANT SELECT, INSERT ON TABLE tz_table TO you;

### Spin up airflow(run in wiki virtual env)
- export AIRFLOW_HOME={dir_path}./airflow
- create admin user to interact with web ui on http://localhost:8080/
- airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
- pip install requirements.txt 
- Run cd ariflow && airflow webserver in a terminal
- Run cd ariflow && airflow scheduler in a terminal


In the dir ./airflow/dags/schedule_data_consumption.py file change the variable bash command to the right directory level of wiki virtual environment directory and change the directory of stream_consumer.py file the right directory on local.
        **bash_command**="source ./wiki/bin/activate &&  python3 ./spark/stream_consumer.py" -> to the right directory level location of the dag folder
