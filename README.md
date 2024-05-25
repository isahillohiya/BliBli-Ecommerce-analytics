<p align="center">
    <img src="resources/images/intro.gif" width="400" alt="retail gif">
    <h2 align="center">Ecommerce Analytics</h2>
    <p align="center"><a href="https://www.linkedin.com/in/isahillohiya/" target="_blank"><img src="https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" alt="Linkedin"></a>
   
    
</p> 
<p align="center">A end to end ELT data engineering project using fivetran,dbt,snowflake, and airflow. </b></p>

## Overview
<img src="resources/images/architecture_diagram.png" alt="architecture_diagram">

This project establishes a robust, automated data pipeline that refreshes weekly.



Here's a breakdown of the key components:

- Orchestration: Apache Airflow acts as the central scheduler, triggering the pipeline execution on a weekly basis. <br/> <br/>
- Data Extraction: Fivetran is employed to extract data from various sources. It streamlines data retrieval, ensuring a reliable and efficient process.  <br/> <br/>
- Data Storage: The extracted data is securely stored in Snowflake, a cloud-based data warehouse. Snowflake offers scalability and performance for housing large datasets.  <br/> <br/>
- Data Transformation: Upon successful data extraction, Airflow triggers a dbt (data build tool) job. This job performs the final transformations on the data, preparing it for analysis and consumption.  <br/> <br/>

## Data Model 

<img src="resources/images/ER_DIAGRAM.png" alt="ER_DIAGRAM">

