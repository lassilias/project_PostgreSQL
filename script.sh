#!/bin/bash
#wget -O results.csv https://raw.githubusercontent.com/martj42/international_results/master/results.csv
#wget -O shootouts.csv https://raw.githubusercontent.com/martj42/international_results/master/shootouts.csv
make build
#docker volume create shared_volume
docker-compose up -d
until [ "`docker inspect -f {{.State.Running}} project_postgresql_dataBase_1`" == "true" ]; do
    sleep 0.1;
done;
until [ "`docker inspect -f {{.State.Running}} test_container_test`" == "true" ]; do
    sleep 0.1;
done;
./wait-for-it.sh project_postgresql_dataBase_1:5432 -- echo "database is up and running"
docker cp credential.csv project_postgresql_dataBase_1:/home/credentials.csv
docker exec -it test_container_test  bash -c "airflow tasks test etl_LASSOULI extract_and_transform 2021-01-01"
docker exec -it project_postgresql_dataBase_1 psql -U postgres postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'INTERNATIONAL_RESULTS'" | grep -q 1 || docker exec -it project_postgresql_dataBase_1 psql -U postgres postgres -c "CREATE DATABASE INTERNATIONAL_RESULTS"
docker exec -it test_container_test  bash -c "airflow tasks test etl_LASSOULI TEST 2021-01-01"
docker exec -it test_container_test  bash -c "airflow tasks test etl_LASSOULI mapping_pandas_sql 2021-01-01"
docker exec -it test_container_test  bash -c "airflow tasks test etl_LASSOULI create_table_postgres_external_file 2021-01-01"
docker exec -it test_container_test  bash -c "airflow tasks test etl_LASSOULI create_table_mysql_external_file2 2021-01-01"
docker exec -it test_container_test  bash -c "airflow tasks test etl_LASSOULI create_table_mysql_external_file3 2021-01-01"
# airflow trigger_dag
#docker exec -i project_postgresql_dataBase_1 mysql -u root -psupersecret < dummy.sql
docker exec -it  test_container_test  bash -c "python3 main.py"

