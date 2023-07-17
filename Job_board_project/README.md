 

## **Project Name: Job Board ETL Project**

## **Team Name: GROUP 4**

 ## **Document Version 1**

**DATE : Feb 16, 2023**

## **Revision History**

| **Version** | **Authors**           | **Date**  | **Description** |
|-------------|-----------------------|-----------|-----------------|
|             |**Daniel Marinescu**   |Feb/16/2023|                 |
|        1.0  |**Vasanth Nair**       |           |                 |
|             |**Christian Freeman**  |           |                 |

## **Index**
<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
   <li>
      <a href="#goals">Goals</a>
    </li>
   <li>
      <a href="#project-context">Project Context</a>
    </li>
   <li>
      <a href="#architecture">Architecture</a> 
     <ul>
        <li><a href="#etl-pipeline-steps">ETL Pipeline Steps</a></li>
      </ul>
   </li>
   <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
      </ul>
      <ul>
        <li><a href="#installation">Installation</a></li>
      </ul>
      <ul>
        <li><a href="#running-locally-and-in-a-docker-container">Running Locally and in a Docker Container</a></li>
      </ul>
      <ul>
        <li><a href="#running-in-aws-cloud---setup">Running in AWS Cloud - Setup</a></li>
      </ul>
      <ul>
        <li><a href="#screenshots-of-aws-components-used">Screenshots of AWS Components Used</a></li>
      </ul>
      <ul>
        <li><a href="#screenshots-of-datasets">Screenshots of Datasets</a></li>
      </ul>
      <ul>
        <li><a href="#screenshots-of-metadata-logs">Screenshots of Metadata Logs</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project


This project aims to build a scalable and efficient ETL pipeline that can incrementally extract Jobs data 
(mainly data-related jobs like data analyst, data engineer, and data scientist) from an external API. The project requires
a solution that can handle large amounts of data while ensuring the integrity and accuracy of the information. 
The product will be a centralized repository of live data that can be easily queried and analyzed. Using a Postgres database, 
we ensure that the data is stored in a structured and secure manner, allowing for efficient retrieval and manipulation of the information. The final result should be hosted on AWS, taking advantage of its various services to ensure the robustness and reliability of the pipeline. 


<p align="right">(<a href="#readme-top">back to top</a>)</p>



### Built With

The team used a variety of tools in this project, including `Postgres`,`Python`, `Git`,`Docker`,`AWS` and `Visual Studio`.
 

* ![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white) -- **`Postgres`** was used as the primary database to store and manage the data extracted from the pipeline.


* ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) -- **`Python`** was used for developing custom scripts to perform data transformations and manipulation. Its powerful libraries, such as Pandas and NumPy, were utilized for data manipulation.


* ![Git](https://img.shields.io/badge/git-%23F05033.svg?style=for-the-badge&logo=git&logoColor=white)  -- **`Git`** was used for version control to manage the codebase and collaborate with other team members.


* ![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white) -- **`Docker`** was used to create isolated environments for development, testing, and production, allowing for easy and efficient deployment of the applications.


* ![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white) -- **`AWS`** was used as the cloud platform to host the applications, store, and leverage various services for data hosting.


* ![Visual Studio Code](https://img.shields.io/badge/Visual%20Studio%20Code-0078d7.svg?style=for-the-badge&logo=visual-studio-code&logoColor=white)  -- **`Visual Studio`** was used as the integrated development environment (IDE) to write and debug code, as well as to collaborate with other team members.



<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- GOAls -->
## Goals

<p>Building a live and incremental data pipeline solution is to provide a system that can process and deliver data to consumers as quickly as possible. By leveraging real-time data processing, data can be continuously ingested and transformed, ensuring that the data is always up-to-date and available to data consumers with minimal latency. This pipeline solution allows data consumers to work with the most recent data, which is especially important in fast-paced business environments, where real-time data can make all the difference in making critical decisions.</p>

<p>The incremental approach that a live and incremental data pipeline solutions utilize also allows for the seamless addition of new data sources, as the system can be designed to identify and ingest new data automatically. This feature ensures that the system is scalable and can handle large amounts of data with minimal impact on performance. Overall, building a live and incremental data pipeline solution aims to create a scalable, reliable, and high-performing data processing system that enables data consumers to make informed decisions based on the latest data available.</p>



<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- PROJECT CONTEXT -->
## Project Context

<p>Technical recruiters and head-hunting firms in data-relevant fields are constantly in need of not only top candidates, but information about what positions companies are looking to fill for given data roles and all other details pertaining to these roles including salary, technologies, years of experience etc to list a few. With this ETL Job Board project, we would like to help Technical recruiters and head-hunting firms with accurate, relavant and up-to-date data related to data specific roles namely Data Analyst, Data Engineer and Data Scientist.
</p>



<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ARCHITECTURE -->
## Architechture

<img width="1103" alt="image" src="https://user-images.githubusercontent.com/1815429/219284593-c615eb95-4947-4d7d-a165-e73b9f537b42.png">


### ETL Pipeline Steps

1. An AWS EC2 Instance boots up and downloads the job_board etl pipeline app docker image from AWS ECR.
2. AWS EC2 instance runs this docker image in a docker container.
3. Docker container reads ENV file from AWS S3 Bucket.
4. It sets the read contents and set them as environment variable making it available for ETL program during runtime.
5. On a scheduled time, ECS cron job kicks in and starts the ETL pipeline.
6. The python job_board ETL pipeline makes a REST API call to JSEARCH API to get jobs data specific to data roles namely data analyst, data engineering, data scientist.
7. Response data from the API call is extracted and transformed, and also enriched with a local csv file containing regions information.
8. This transformed and enriched data is loaded to Postgres DB in AWS RDS in 3 different tables namely `data_analyst_job`, `data_engineer_job` and `data_scientist_job`. The metadata logs of scheduled ETL runs and their statuses are saved to their corresponding metadata tables namely `metadata_log_data_analyst_job`, `metadata_log_data_engineer_job` and `metadata_log_data_scientist_job` respectively. 

<!-- GETTING STARTED -->
## Getting Started

### Prerequisites

_This project requires following softwares, packages and tools._

  1. **Python 3.9** to write the **JOB_BOARD** ETL pipeline code
  2. **Postgres DB 13** or above to store the extracted and transformed data from the **JOB_BOARD** ETL pipeline job
  3. **Docker** to containerize the **JOB_BOARD** ETL pipeline application
  4. **AWS** to host various components of the **JOB_BOARD** ETL Pipeline application

  
### Installation

_Below are the installation steps for setting up the job_board ETL app._

1. Get a Paid API Key at [https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch](https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch)
2. Clone the repo
   ```sh
   git clone  https://github.com/mddan/job_board.git
   ```
3. Install packages
   ```sh
   pip install  -r requirements.txt
   ```
4. Create ```set_python_path.sh``` / ```set_python_path.bat``` file  in ```src/``` folder with following contents
   
   **Linux / Mac**
   ```
   #!/bin/bash

   export PYTHONPATH=`pwd`
   ```
   
   **Windows**
   ```
   set PYTHONPATH=%cd%
   ```
5. Create a ```config.sh``` / ```config.bat``` file in ```src/job_board``` folder with following content 

   **Linux / Mac**
   ```
   export api_key_id=<YOUR_JSEARCH_API_KEY>
   export db_user=<YOUR_POSTGRES_DB_USERNAME>
   export db_password=<YOUR_POSTGRES_DB_PASSWORD>
   export db_server_name=<YOUR_POSTGRES_DB_SERVER_NAME>
   export db_database_name=<YOUR_POSTGRES_DB_NAME>
   export db_port=<YOUR_POSTGRES_DB_PORT>
   ```
   
   **Windows**
   ```
   SET api_key_id=<YOUR_JSEARCH_API_KEY>
   SET db_user=<YOUR_POSTGRES_DB_USERNAME>
   SET db_password=<YOUR_POSTGRES_DB_PASSWORD>
   SET db_server_name=<YOUR_POSTGRES_DB_SERVER_NAME>
   SET db_database_name=<YOUR_POSTGRES_DB_NAME>
   SET db_port=<YOUR_POSTGRES_DB_PORT>
   ```
6. Create a ```.env``` file with below contents in root project folder

```
api_key_id=<YOUR_JSEARCH_API_KEY>
db_user=<YOUR_POSTGRES_DB_USERNAME>
db_password=<YOUR_POSTGRES_DB_PASSWORD>
db_server_name=localhost
db_database_name=job_board
db_port=5432
```

### Running Locally and in a Docker Container

#### Steps

1. CD into ```src/``` folder
2. Run ```. ./set_python_path.sh``` / ```set_python_path.bat``` file according to your Operating System to set **PYTHONPATH**
3. Run ```config.sh``` / ```config.bat``` file to set additional environment variables needed to connect to **JSEARCH API** and **POSTGRES DB**
4. CD back to ```src/``` folder  
5. Run ```python job_board/pipeline/job_board_pipeline.py``` to run the ETL pipeline locally.
6. Alternatively instead of running steps 3 thru 5, we can run the ETL pipeline in docker container as follows. 
7. From the root folder containing ```Dockerfile```, Run ```docker build -t job_board:1.0 .``` to create a docker image for Job Board ETL pipeline program
8. Run a container using the above image using ```docker run --env-file=.env job_board:1.0``` to see the ETL pipeline in action.

### Running in AWS Cloud - Setup

1. Create IAM roles as shown in image.
2. Create an RDS Instance with public visibility, inbound and outbound connections setup.
3. Upload the .env file containing the JSEARCH API Key and AWS RDS Connection Details to an AWS S3 Bucket.
4. Create docker file and upload the Docker image to AWS ECR.
5. Create a Cron Schedule in AWS ECS to run the pipeline in a recurring schedule.

### Screenshots of AWS Components Used

#### IAM Roles Used

<img width="1062" alt="image" src="https://user-images.githubusercontent.com/1815429/219292235-68866722-e034-4956-8a0e-5d11e916c3fa.png">

#### RDS Database Instance

<img width="1105" alt="image" src="https://user-images.githubusercontent.com/1815429/219291798-80016e01-22ba-48eb-9822-5043fa36cf51.png">

#### Env File in S3 Bucket

<img width="1068" alt="image" src="https://user-images.githubusercontent.com/1815429/219292301-6e6e78cb-1315-4c24-a368-d0a2beab3017.png">

#### ECR hosting Job Board Docker Image

<img width="1103" alt="image" src="https://user-images.githubusercontent.com/1815429/219292060-7fbb1595-00be-4ad0-80c2-a40c1f39cf1e.png">

#### Scheduled Task in ECS 

<img width="1184" alt="image" src="https://user-images.githubusercontent.com/1815429/219291967-9170e0e6-5dc5-43b1-b69d-87b10e9f40fc.png">

### Screenshots of Datasets

#### Data Analyst Jobs Dataset

<img width="1440" alt="image" src="https://user-images.githubusercontent.com/1815429/219293243-2a6c5465-feb2-408a-83c6-6b882c8fe419.png">

#### Data Engineer Jobs Dataset

<img width="1440" alt="image" src="https://user-images.githubusercontent.com/1815429/219293163-05dc94bb-5433-4677-93f1-715e1cbf2885.png">

#### Data Scientist Jobs Dataset

<img width="1440" alt="image" src="https://user-images.githubusercontent.com/1815429/219293048-fb79cf88-b441-4ad1-a645-af4a1b704849.png">

### Screenshots of Metadata Logs

#### Data Analyst Metadata Log

<img width="1440" alt="image" src="https://user-images.githubusercontent.com/1815429/219292972-5fe8eee5-ec24-45d7-921a-b1fc04755a9f.png">

#### Data Engineer Metadata Log

<img width="1440" alt="image" src="https://user-images.githubusercontent.com/1815429/219292846-8622fa47-9ab7-4554-ba31-002c44fce703.png">

#### Data Scientist Metadata Log

<img width="1440" alt="image" src="https://user-images.githubusercontent.com/1815429/219292701-0e88fe5e-dcd9-448f-b970-85f822575301.png">


<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- USAGE EXAMPLES -->
## Usage

An ETL data pipeline solution is essential for collecting, transforming, and loading data from various sources into a centralized repository. The pipeline benefits data analysts, data scientists, and other data consumers in several ways. It centralizes the data for easy accessibility, standardizes and ensures data quality, consistency, and accuracy, and automates the process of data transformation and scaling up as the data volume grows. Consequently, data consumers can quickly access high-quality, consistent, and easily accessible data for making informed decisions.
 

 

<p align="right">(<a href="#readme-top">back to top</a>)</p>

 
<!-- ROADMAP -->
## Roadmap

- [X] **`Data extraction:`**
    - [X] Set up API for data extraction 
    - [X] Retrieve the Jobs data from the API using a suitable extraction method (API calls)
    - [X] Set up live data update and incremental extract 
- [X] **`Data transformation:`**
    - [X] Clean the raw data to ensure it is in the desired format (e.g., removing duplicates, handling missing values, etc.).
    - [X] Use the following transformation techniques :  renaming columns, joining, grouping, typecasting, data filtering, sorting, and aggregating 
    - [X] Filter the data only to include the desired jobs (e.g., data analyst, data engineer, and data scientist).
    - [X] Transform the data into a structured format (e.g., converting to a tabular form or creating a data model).
- [X] **`Data loading:`**
    - [X] Create the necessary tables and schemas in the Postgres database to store the data
    - [X] Load the transformed data into the database.
    - [X] Use an efficient data loading method (e.g., upsert, etc.) to populate the database.
    - [X] Set up Incremental and upsert load
- [X] **`Create a data Pipeline`**
    - [X] Build a docker image using a Dockerfile
    - [X] Test that the Docker container is runing locally
- [X] **`Incremental extraction and loading:`**
    - [X] Set up a process to regularly extract new data from the API and update the database with the latest information.
    - [X] Ensure that the incremental process is designed to handle large amounts of data and maintain the integrity and accuracy of the information.
- [X] **`Implement unit tests`**
    - [X] Write pipeline metadata logs to a database table
- [X] **`Data Hosting :`**
    - [X] Host the database on AWS
    - [X] Use AWS services (e.g., RDS, EC2, S3, ECR, ECS etc.) to ensure the robustness and reliability of the pipeline.

 
<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

**Daniel Marinescu** - [@Linkedin](https://www.linkedin.com/in/danielmarinescu2/) 

**Vasanth Nair** - [@Linkedin](https://www.linkedin.com/in/vasanthnair/) 

**Christian Freeman** -  [@Linkedin](https://www.linkedin.com/in/christian-freeman-r/)

**Project Link:** [https://github.com/mddan/job_board](https://github.com/mddan/job_board)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

Use this space to list resources you find helpful and would like to give credit to. I've included a few of my favorites to kick things off!

* [Choose an Open Source License](https://choosealicense.com)
* [GitHub Emoji Cheat Sheet](https://www.webpagefx.com/tools/emoji-cheat-sheet)
* [Malven's Flexbox Cheatsheet](https://flexbox.malven.co/)
* [Malven's Grid Cheatsheet](https://grid.malven.co/)
* [Img Shields](https://shields.io)
* [GitHub Pages](https://pages.github.com)
* [Font Awesome](https://fontawesome.com)
* [React Icons](https://react-icons.github.io/react-icons/search)
* [Excalidraw](https://excalidraw.com/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/othneildrew/Best-README-Template.svg?style=for-the-badge
[contributors-url]: https://github.com/othneildrew/Best-README-Template/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/othneildrew/Best-README-Template.svg?style=for-the-badge
[forks-url]: https://github.com/othneildrew/Best-README-Template/network/members
[stars-shield]: https://img.shields.io/github/stars/othneildrew/Best-README-Template.svg?style=for-the-badge
[stars-url]: https://github.com/othneildrew/Best-README-Template/stargazers
[issues-shield]: https://img.shields.io/github/issues/othneildrew/Best-README-Template.svg?style=for-the-badge
[issues-url]: https://github.com/othneildrew/Best-README-Template/issues
[license-shield]: https://img.shields.io/github/license/othneildrew/Best-README-Template.svg?style=for-the-badge
[license-url]: https://github.com/othneildrew/Best-README-Template/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/othneildrew
[product-screenshot]: images/screenshot.png
[Next.js]: https://img.shields.io/badge/next.js-000000?style=for-the-badge&logo=nextdotjs&logoColor=white
[Next-url]: https://nextjs.org/
[React.js]: https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB
[React-url]: https://reactjs.org/
[Vue.js]: https://img.shields.io/badge/Vue.js-35495E?style=for-the-badge&logo=vuedotjs&logoColor=4FC08D
[Vue-url]: https://vuejs.org/
[Angular.io]: https://img.shields.io/badge/Angular-DD0031?style=for-the-badge&logo=angular&logoColor=white
[Angular-url]: https://angular.io/
[Svelte.dev]: https://img.shields.io/badge/Svelte-4A4A55?style=for-the-badge&logo=svelte&logoColor=FF3E00
[Svelte-url]: https://svelte.dev/
[Laravel.com]: https://img.shields.io/badge/Laravel-FF2D20?style=for-the-badge&logo=laravel&logoColor=white
[Laravel-url]: https://laravel.com
[Bootstrap.com]: https://img.shields.io/badge/Bootstrap-563D7C?style=for-the-badge&logo=bootstrap&logoColor=white
[Bootstrap-url]: https://getbootstrap.com
[JQuery.com]: https://img.shields.io/badge/jQuery-0769AD?style=for-the-badge&logo=jquery&logoColor=white
[JQuery-url]: https://jquery.com 

