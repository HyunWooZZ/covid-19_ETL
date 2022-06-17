FROM apache/airflow:2.3.2-python3.8
COPY /plugins/requirements.txt /requirements.txt

RUN python3 -m pip install --user --upgrade pip
RUN python3 -m install --no-cache-dir --user -r /requirements.txt