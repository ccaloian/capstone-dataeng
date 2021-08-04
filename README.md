# US Demographics and Immigration

This is the capstone project for the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## 1. Scope the Project and Gather Data

### 1.1 Scope 

The goal of the project is to run aggregate analysis on the demographic information from 2015 and US visitors in 2016. The data is stored in a data lake in AWS S3 and to ETL solution is implemented in Apache Spark. On the final (processed) dataset one can calculate summary statistics such as total population per race per state, number of visitors entering each state per country of resicence or per country of citizenship, number of visitors per visa type in each state, visa duration, etc. 

### 1.2 Data 

The __US Demographics Data__ comes from the US Census Bureau's 2015 American Community Survey and was downloaded from [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/?dataChart=eyJxdWVyaWVzIjpbeyJjb25maWciOnsiZGF0YXNldCI6InVzLWNpdGllcy1kZW1vZ3JhcGhpY3MiLCJvcHRpb25zIjp7fX0sImNoYXJ0cyI6W3siYWxpZ25Nb250aCI6dHJ1ZSwidHlwZSI6ImNvbHVtbiIsImZ1bmMiOiJBVkciLCJ5QXhpcyI6ImNvdW50Iiwic2NpZW50aWZpY0Rpc3BsYXkiOnRydWUsImNvbG9yIjoicmFuZ2UtY3VzdG9tIn1dLCJ4QXhpcyI6InN0YXRlIiwibWF4cG9pbnRzIjo1MCwic29ydCI6IiIsInNlcmllc0JyZWFrZG93biI6InJhY2UifV0sInRpbWVzY2FsZSI6IiIsImRpc3BsYXlMZWdlbmQiOnRydWUsImFsaWduTW9udGgiOnRydWV9). This dataset contains information about the demographics of all US cities and census designated places with a population greater or equal to 65,000. It records information such as age, gender, household, race, and population counts. The dataset is in `csv` format and contains 2,891 rows and 12 columns. For a full description of the data see [DEMOGRAPHICS_README.txt](./DEMOGRAPHICS_README.txt)


The __I94 Immigration Data__ is from 2016 and is downloaded from [here](https://travel.trade.gov/research/reports/i94/historical/2016.html). This dataset contains information about non-US residents visiting US during 2015. It records information such as date of entry in US, country of citizenship, country of residence, destination (state), visa type, The original dataset is in `sas` format, one file per month. The full dataset contains 40,790,529 rows and 28 columns. For a full description of the data see [I94_SAS_README.txt](./I94_SAS_README.txt).

---

## 2. Explore and Assess the Data

### 2.1 Explore the Data 

Exploratory Data Analysis steps are contained in the `eda.ipynb` notebook.

### 2.2 Cleaning Steps

#### US Demographics

This dataset is clean, meaning that there are very few `null` entries (not in columns of interest) and no duplicate rows. The main processing step is mainly to rearrange the data, _i.e._ pivot the _Race_ column, to obtain a dataset with _City_ and _State_ entries as rows and demographic information as columns.

#### I94 Immigration

For this dataset a lot more processing was required. The main steps are outlined below:
* Read data from `sas` format and cast to the correct type
* Write data to `parquet` file partitioned by `i94mon` (month) to be easily accessible
* Replace SAS codes with corresponding information
* Convert SAS time format to date type
* Convert strings to date type and handle corner cases, _e.g._ year 9999

---

## 3. Define the Data Model

### 3.1 Conceptual Data Model
_Map out the conceptual data model and explain why you chose that model._

#### Processed Demographics Data Model

| Column | Type | Description |
|:---|:---|:---|
| `state` | StringType | Full state name |
| `state_code` | StringType | Two characters state code |
| `num_cities` | LongType | Number of cities considered in state |
| `total_pop` | DoubleType | Total population |
| `amind_pop` | LongType | Total American Indian and Alaska Native population |
| `asian_pop` | LongType | Total Asian population |
| `afram_pop` | LongType | Total Black or African-American population |
| `hispl_pop` | LongType | Total Hispanic or Latino population |
| `white_pop` | LongType | Total White population |

This data model is different than the raw data by the fact that the `Race` column was pivoted so that for each state we get one column per `Race` value with it's corresponding population count. The reason to choose this model is that we are interested on state aggregations. A finer aggregation, per city, is also possible but it would require more work to match the demographics and immigration city entries. 

#### Processed I94 Immigration Data Model

| Column | Type | Description |
|:---|:---|:---|
| `cicid` | IntegerType | CIC Id number |
| `i94yr` | IntegerType | Year |
| `i94mon` | IntegerType | Month |
| `i94cit` | StringType | Citizenship |
| `i94res` | StringType | Residence |
| `i94port` | StringType | Port |
| `arrdate` | DateType | Arrival Date in US |
| `i94addr` | StringType | Address in US |
| `depdate` | DateType | Departure Date from US |
| `i94bir` | IntegerType | Age of Respondent in Years |
| `i94visa` | StringType | Visa Category |
| `count` | IntegerType | 1.0 (used for summary statistics) |
| `dtadfile` | DateType | Date added to I-94 Files (yyyyMMdd) |
| `biryear` | IntegerType | 4 digit year of birth |
| `dtaddto` | DateType | Date to which admitted to US (MMddyyyy) |
| `gender` | StringType | Non-immigrant sex |
| `visatype` | StringType | Visa Type |
| `state` | StringType | Two digit state code |

This data model differs from the raw data by the fact that some columns were dropped, and for the remaining columns the codes were substituted by the explicit values and they were converted to the right type. The selected columns gives enough information to calculate aggregations per state, visa type, destination in US, country of residence, country of citizenship, and calculate statistics such as remaining visa days. 

The two datasets are joined on the two digit state code.

### 3.2 Mapping Out Data Pipelines
The steps necessary to pipeline the data into the chosen data model are as follows:

#### US Demographics

* Group by `City` and `State`
* Count number of cities
* Pivot `Race` column
* For each `City` and `Race` pair gather the corresponding `Count`

#### I94 Immigration

* Read data from `sas` format 
* Cast `sas` data to the correct type
* Write data to `parquet` file partitioned by `i94mon` (month) to be easily accessible
* Keep only columns of interest
* Replace SAS codes with corresponding information
* Convert SAS time format to date type
* Convert strings to date type and handle corner cases, _e.g._ year 9999

---

## 4 Run Pipelines to Model the Data 

### 4.1 Create the data model
Build the data pipelines to create the data model.

### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
 ### 4.3 Data dictionary 
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

---

## 5 Discussion

### Tools and technologies

For this project I chose to implement the data source as a data lake in AWS S3, and the ETL pipeline in Apache Spark. While the same pipeline can be implemented in a traditional data warehouse, this option is not as flexible. In general one needs to know in advance the kind of queries would be ran and based on that implement an operational data model. The architecture might be difficult to adapt to further changes in the requirements.

Using Apache Spark and data lake architecutre we gain a lot of flexibility in the sense that we can work directly on the raw data and benefit from the schema-on-read capabilities. This way we can adapt quickly to requirement changes. One other advantage is that the pipeline can be easily shared with a larger audience due to the `spark.sql` engine that allows us to write queries using the DataFrame API or the SQL API.

### Data updates and maitenance

Given the current scenario, the demographics data could be updated every year, whereas the immigration data can be updated much more frequently, depending on the use case.

As discussed above, Apache Spark makes it easy for us to adapt to changes in requirements and implement new transformations and queries.

### Scenarios

#### The data was increased by 100x

This scenario is naturally handled by Apache Spark. We can scale the Spark cluster both horizontally and vertically to handle the new data requirements. This data volume increase is scalable also with the AWS S3 solution. 

#### The data populates a dashboard that must be updated on a daily basis by 7am every day.

In this case we would need a scheduling system that would run the pipeline every morning and push the results to a location accessible by the dashboard solution. Apache Airflow would be a good fit for this scenario. 

#### The database needed to be accessed by 100+ people.

Thius depends on the level of the data the users needs. If they need access to upstream data (raw dataset) to perform custom transformations and queries, Apache Spark could be set up on Yarn or Mesos clusters and everyone could access the resources. If the users need to consume already processed data for various summary statistics and visualizations, a main pipeline should be set up, ran on a schedule as discussed above, that outputs an analytical dataset for consumption in a database or some other resource available for everyone.

## 6 How to run

You need a Python 3 environment with the packages in `requirements.txt` installed.

Additionally, Spark must be installed and the `SPARK_HOME` variable must point to the spark installation.

The exploratory data analysis steps are in `eda.ipynb` that you can run using Jupyter notebook.

The data is available in AWS S3 in `s3://udacity-dataeng-datasets`.

Run the ETL pipeline as a Python script:

```
python3 main.py
```

## 7 References

1. [US Census Bureau's 2015 American Community Survey](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/?dataChart=eyJxdWVyaWVzIjpbeyJjb25maWciOnsiZGF0YXNldCI6InVzLWNpdGllcy1kZW1vZ3JhcGhpY3MiLCJvcHRpb25zIjp7fX0sImNoYXJ0cyI6W3siYWxpZ25Nb250aCI6dHJ1ZSwidHlwZSI6ImNvbHVtbiIsImZ1bmMiOiJBVkciLCJ5QXhpcyI6ImNvdW50Iiwic2NpZW50aWZpY0Rpc3BsYXkiOnRydWUsImNvbG9yIjoicmFuZ2UtY3VzdG9tIn1dLCJ4QXhpcyI6InN0YXRlIiwibWF4cG9pbnRzIjo1MCwic29ydCI6IiIsInNlcmllc0JyZWFrZG93biI6InJhY2UifV0sInRpbWVzY2FsZSI6IiIsImRpc3BsYXlMZWdlbmQiOnRydWUsImFsaWduTW9udGgiOnRydWV9)

2. [I-94 Visitor Arrivals Program](https://www.trade.gov/i-94-arrivals-program)