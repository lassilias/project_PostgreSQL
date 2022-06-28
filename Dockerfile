FROM ubuntu:20.04
RUN apt update && apt-get install nano ca-certificates libpsl5 libssl1.1 openssl publicsuffix wget && apt install python3-pip libmysqlclient-dev -y 
RUN pip3 install apache-airflow && pip3 install apache-airflow-providers-mysql && pip3 install apache-airflow-providers-postgres
RUN pip3 install pandas && pip3 install requests && pip3 install pydantic && pip3 install fastapi && pip3 install sqlalchemy && pip3 install uvicorn
RUN pip3 install python-jose[cryptography] && pip3 install install passlib[bcrypt] && pip3 install python-multipart
RUN pip3 install psycopg2-binary
RUN airflow db init 
RUN airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
#COPY credential.csv /home/airflow/credential.csv
WORKDIR ./airflow 
COPY  dags/LASSOULI_dag.py /usr/local/lib/python3.8/dist-packages/airflow/example_dags/
RUN airflow connections add 'postgre_sql_db' \
    --conn-uri 'postgresql://postgres:example@dataBase:5432/international_results'
#COPY results.csv /root/airflow/
#COPY shootouts.csv /root/airflow/
ADD main.py .
ADD get_customers.sql .
CMD airflow scheduler -D
CMD airflow webserver -D 
EXPOSE 8080
EXPOSE 8000
