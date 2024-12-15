# ETL Pipeline for FlightRadar24 API Data

This project is an ETL (Extract, Transform, Load) pipeline to load data from the FlightRadar24 API and process it using the Spark engine.

## Overview

### ETL Logic:
The pipeline follows these steps:
1. **Extraction**:
   - In the `extract` file, we extract all available data from the FlightRadar24 API.
   - The schema follows a **star model**:
     - `flights`: the fact table.
     - `airports`, `airlines`, and `zones`: the dimension tables.

2. **Transformation**:
   - Before loading the data, a cleaning process removes all unused or irrelevant information from the fact table (`flights`).

3. **Loading**:
   - The processed data is saved in a folder, organized by date and hour. This allows data analysts or scientists to access it easily.

---

## KPIs and Analytical Questions

After the ETL process, you can run the KPI scripts in the `KPIs` folder to answer the following questions:

1. **Which airline has the most ongoing flights?**
2. **For each continent, which airline has the most active regional flights?**
   - (Regional flights: origin continent == destination continent)
3. **What is the ongoing flight with the longest route?**
4. **What is the average flight distance for each continent?**
5. **Which aircraft manufacturer has the most active flights?**
6. **For each airline's country, what are the top 3 aircraft models in use?**

---

## Industrialization Considerations

To ensure readiness for production, the ETL process is designed with the following principles:

- **Fault-Tolerant**:
  - The pipeline can handle corner cases and corrupted data without causing job failure.
- **Observable**:
  - Logs are recorded for all critical stages, ensuring the job is monitorable and debuggable.
- **Systematic**:
  - Data and results are stored systematically using a storage hierarchy (`Date`, `Hour`) to enable data analysts to retrieve the exact data they need.

---

## Deployment with Docker

### How it Works:
The pipeline is containerized with Docker. A cron job is integrated into the container to execute the ETL and KPI processes every two hours automatically while the container is running.

### Using the Container:
1. **Install Docker**:
   - [Docker Installation Guide](https://docs.docker.com/get-docker/)
   
2. **Run the Container**:
   ```bash
   docker run -d arabitra/kata_repo:latest
   
2. **Cron JOB**:
    - Once the container is running, the cron job inside it executes the ETL and KPI scripts every 2 hours automatically.

---

## Data Analysts Use Case
To demonstrate how to use the processed data, a Jupyter Notebook is provided in the repository. It contains a use case showing how to answer the analytical questions listed above.

---

## Project Structure:
   ```plaintext
   .
   ├── ETL.py                     # Main ETL script
   ├── KPIs/                      # Folder containing individual KPI scripts
   ├── data/                      # Folder where ingested and processed data is stored
   ├── utils/                     # Utility scripts for loading, saving, and processing
   ├── requirements.txt           # Python dependencies
   ├── Dockerfile                 # Dockerfile for building the container
   ├── cronjob                    # Cron job configuration
   └── README.md                
   ```

---

## Conclusion
This project provides a scalable and fault-tolerant ETL pipeline, along with ready-to-use KPI scripts and containerization for seamless deployment. By leveraging Docker, the solution is portable and can be executed in any environment with minimal setup.

