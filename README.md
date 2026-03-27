# Docker Container and PostgreSQL Database Integration
This project goes over creating a Docker container that contains a Postgres DB and a python script to push data from a csv to that DB.
The code generates 50 rows of fake data using the Faker library.

## Quickstart
1. **Preqs**
    - WSL2 Debian or other Linux instance.
    - Docker CLI or Docker Desktop.
    - Python version 3.10.0
2. **Install**
    - Need to create the virtual python environment.
    ```bash
    python -m venv venv && source venv/bin/activate
    ```
3. **GitHub**
   - Clone the repo using SSH or HTTP:
   ```bash
   git clone git@github.com:jeffmoe/CST_635_Week2.git
   ```
4. **Terminal**  
   Create the secrets file and data location for the csv file:
   ```bash
   mkdir db && touch password.txt
   mkdir data
   ```
5. **Docker**
    - Create the image from the yaml and dockerfile in the repo:
    ```bash
    docker compose up --build
    ```
6. **Postgres**  
   - Once the container runs successfully, check the table to ensure data was uploaded:
    ```bash
    docker exec -it <container_name> psql -U postgres
    \l
    \c <database_name>
    \dt
    \c <table_name>
    SELECT * FROM <table_name>;
    ```
## Notes
- This should allow for your application file to write data from a csv that is created to the DB.
- If you want to pull data from a different source, you will need to edit your yaml and Dockerfiles to use different volumes.




