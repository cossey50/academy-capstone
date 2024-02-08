# Data Minded Academy Capstone

Welcome to the Capstone project! Everything you've learned over the past days 
will now be integrated in a realistic data pipeline. The training wheels are 
off, but we're still at the sideline, cheering you on and supporting you when 
needed.

In a nutshell, here's what you will do:

1) Read, transform and load weather data from S3 to Snowflake through PySpark
2) Take a stab at running your application on AWS through Docker, AWS Batch and Airflow

## Getting started

We've set up a Gitpod environment containing all the tools required to complete
this exercise (awscli, python, vscode, ...). You can access this environment by
clicking the button below:

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/datamindedacademy/willem-capstone)

NOTE: When you fork the code repo to your own remote make sure to change the 
Gitpod URL to reflect your account in this README!

This is an ubuntu-based environment pre-installed with:

- VSCode
- A Python3 virtual environment: we recommend you always work inside this environment.
- The AWS CLI

Before you start data crunching, set up your `$HOME/.aws/credentials` file. This is 
required to access AWS services through the API. To do so, run `aws configure` in 
the terminal and enter the correct information: 

```shell
AWS Access Key ID [None]: [YOUR_ACCESS_KEY]
AWS Secret Access Key [None]: [YOUR_SECRET_KEY]
Default region name [None]: eu-west-1
Default output format [None]: json
```

IMPORTANT: Create a new branch and periodically push your work to the remote. 
After 30min of inactivity this environment shuts down and you will likely lose 
unsaved progress. As stated before, change the Gitpod URL to reflect your remote.

## Task 1: Extract, transform and load weather data from S3 to Snowflake

Our team recently ingested Belgian weather data from the 
[openaq](https://openaq.org/) API and stored it on AWS S3 under 
`s3://dataminded-academy-capstone-resources/raw/open_aq/`.

You are tasked with building a PySpark application that reads this data, 
transforms it and stores it in a Snowflake Table for further analysis. 

### Step 1: Extract and transform

Required transformations:

- Flatten all nested columns
  Several databases and data warehouses don't work well with nested columns. 
  And even if they do, many analysts don't know how to pick the right 
  information from them. So having non-complex 2 dimensional tables is a boon.
- Some columns don't have the right datatype. Fix this, so that downstream 
  users may benefit from more efficient data types and related functionality.

You can access the weather data through your AWS credentials. While working locally, you'll need to
pass these to your spark application. In addition, your spark application requires 
the following JAR to interact with AWS S3 `org.apache.hadoop:hadoop-aws:3.1.2`.

### Step 2: Retrieve Snowflake credentials

After you've successfully extracted and transformed the data, you can move on to loading said data to Snowflake.
To this end, you'll require Snowflake credentials stored on AWS Secretsmanager 
under the following secret `snowflake/capstone/login`.

Write Python code that retrieves this secret to leverage in the next step.

### Step 3: Load to Snowflake

The final step requires you to load the transformed data to a Snowflake table utilizing the login details
obtained in the previous step. Your Spark application will also require additional JARs:

- `net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1`
- `net.snowflake:snowflake-jdbc:3.13.3`

Scan the Snowflake docs to figure out how to load data through Spark. All 
required configuration is stored in the AWS secret except the following:

- Snowflake Schema: On Snowflake, we created individual schema's for every
  participant. Write to your schema. The schema name is based on your 
  username so login to Snowflake to retrieve it.
- Table name: Feel free to use any table name you want

When everything's tied together and functional, you can proceed to the next task.

## Task 2: Run your PySpark application on the cloud

As a data engineer, you might run your application locally during development (as you have done during the first task)
but you should always run your applications on a stable, scalable environment with scheduling in place and
ideally deployed through CI/CD.

During the final step of this Capstone you will run your application on AWS Batch scheduled through Airflow (MWAA).
This requires you to execute the following steps:

1. Containerize your application through Docker
2. Create an ECR repository and push your image
3. Create a batch job definition which runs your application (and test it for good measure)
4. Create and upload an Airflow DAG that triggers a batch job using your job definition to a pre-created MWAA environment
5. ??
6. Profit

IMPORTANT NOTES:

- While using the AWS console, make sure you are in the `eu-west-1` region. This is selected in the top right corner of the website.
- Most resources you create will require a name that starts with your AWS username
- Where applicable, you will need to tag every resource you create with:
  
  `environment`: `academy-capstone-winter-2024`

### Step 1: Containerize

Create a `Dockerfile` that packages your application. You can start from one of our Data Minded images
which pre-installs Spark and its dependencies: put `FROM public.ecr.aws/datamindedacademy/capstone:v3.4.1-hadoop-3.3.6-v1` at the top of your Dockerfile.

### Step 2: Push your image to an ECR repository

Through the AWS console, create a private ECR repository. Its name should start with your AWS username.
After creation, push your docker image to this repo.

### Step 3: Create a batch job definition

With your image pushed to the repository, navigate to AWS Batch and create a new Job Definition. Apply the following configuration:

- Name: Make sure it starts with your AWS username
- Platform type: EC2
- Execution role: `academy-capstone-winter-2024-batch-job-role`
- Job configuration:
    - Image: Image you pushed during the previous step
    - Command: Depends on your image:)
    - Job Role Configuration: `academy-capstone-winter-2024-batch-job-role`
- Tags:
  - `environment`: `academy-capstone-winter-2024`
  
After creating the job definition you can run it by submitting a new job. Again, apply the correct naming convention and tags. 
You can submit the job to the following queue: `academy-capstone-winter-2024-job-queue`

### Step 4: Scheduling through MWAA

To conclude this part, create a DAG that triggers your AWS Batch job and upload it to an MWAA environment created for you.
You will find your environment by navigating to MWAA in the AWS console under the name `shared-mwaa-env`. Upload your DAG to the DAG folder specified in your MWAA environment.
You can access the Airflow Web UI through the link in the console.

After a successful upload, your DAG should be visible in the Airflow UI and can be triggered.

## Bonus 1: Writing and scheduling an air quality data ingest job

In case you finished the capstone but want to expand your pipeline, feel free to create an ingest job which fetches air quality data and stores it in S3.
To this end, have a look at the [openaq project](https://openaq.org/#/). They expose a public API which can be called to retrieve air quality metrics filtered on a set of parameters. (Note: We used this API to gather the raw files you transformed and loaded into Snowflake)
You can ingest your data to the following S3 location `s3://dataminded-academy-capstone-resources/{YOUR_USER_NAME}/ingest/`

## Bonus 2: Build dashboards through SQL queries on Snowsight

With the transformed weather data available on Snowflake, login on Snowflake and navigate to Snowsight.
The team wants you to perform a couple analysis through SQL and build an insightful graph for each of them:

1) Show the monthly maximum values of each air quality metric
2) Compare the daily max of each metric to its 7-day historic maximum. You will require window functions

Feel free to tackle this however you want and we are here to help!
