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
* Replace SAS codes with corresponding information
* Convert SAS time format to date type
* Convert strings to date type and handle corner cases, _e.g._ year 9999
* Keep only columns of interest

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

## 5 Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
* Propose how often the data should be updated and why.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 * The database needed to be accessed by 100+ people.

