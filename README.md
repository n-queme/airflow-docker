# Airflow Orchestration

## Licence
<p xmlns:cc="http://creativecommons.org/ns#" xmlns:dct="http://purl.org/dc/terms/"><span property="dct:title">Airflow Orchestration</span> by <a rel="cc:attributionURL dct:creator" property="cc:attributionName" href="https://github.com/n-queme">Nathan Queme</a> is licensed under <a href="https://creativecommons.org/licenses/by-nc-sa/4.0/?ref=chooser-v1" target="_blank" rel="license noopener noreferrer" style="display:inline-block;">Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International<img style="height:22px!important;margin-left:3px;vertical-align:text-bottom;" src="https://mirrors.creativecommons.org/presskit/icons/cc.svg?ref=chooser-v1" alt=""><img style="height:22px!important;margin-left:3px;vertical-align:text-bottom;" src="https://mirrors.creativecommons.org/presskit/icons/by.svg?ref=chooser-v1" alt=""><img style="height:22px!important;margin-left:3px;vertical-align:text-bottom;" src="https://mirrors.creativecommons.org/presskit/icons/nc.svg?ref=chooser-v1" alt=""><img style="height:22px!important;margin-left:3px;vertical-align:text-bottom;" src="https://mirrors.creativecommons.org/presskit/icons/sa.svg?ref=chooser-v1" alt=""></a></p>


#### Steps to configure Airflow locally on your computer with Docker
1. Build the docker image
```docker-compose build```

2. Start Airflow services
Run this the first time only
```docker-compose up airflow-init```

Run this after initialization
```docker-compose up```

3. Check which containers are running
```docker ps```

4. After a few minute(s), navigate to http://localhost:8080

5. Login with the username/password: `airflow/airflow`


#### Useful commands
1. Using the Airflow CLI
```docker exec <Container ID> <Command>```
For instance to list the current dags run:
```docker exec efg123 airflow dags list```
To list dags which aren't part of the UI run:
```docker exec efg123 airflow dags list-import-errors```

2. Using the Airflow API
```curl -X GET --user "<username>:<password>" "<endpoint>"```
For instance to get the list of dags:
```curl -X GET --user "airflow:airflow" "http://localhost:8080/api/v1/dags"```
