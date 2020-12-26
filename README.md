# Data Engineering Project Notebook

## Group: Data Analyzers

    1. Islam Nasr.
    2. Gaser Khaled.
    3. Kariman Hossam.
    4. Marwan Karim.
    
This repository contains a notebook that demonstates our data engineering process for three datasets, which are:
    1. [World Happiness Report](https://www.kaggle.com/unsdsn/world-happiness)
    2. [All 250 Country Data](https://www.kaggle.com/souhardyachakraborty/all-250-country-data)
    3. [Life Expectancy (WHO)](https://www.kaggle.com/kumarajarshi/life-expectancy-who)

We also integrated with an external dataset in order to get accurate Population figures for the available countries from [here](https://population.un.org/wpp/Download/Standard/Population/).

## Milestone 1:

    1. We imported the 3 datasets.
    2. We explored the datasets.
    3. We performed the needed cleaning and tidying to the datasets.
    4. We dropped the unwanted or unreliable data from the datasets.
    5. We did some data imputation in order to fill in the needed misssing data.
    6. We merged the datasets together into one big dataset which was then used to answer our research questions.
    7. Answering our research questions:
        a. What is the relationship between life expectency and happiness in each region?
        b. What is the happiness factor that contributes the most to the happiness score, based on the Region?
        c. What is the count of countries with life expectancy less/greater than the total average in each year?
        d. Number of deaths by Measles in each region.
        e. What is the ranking of Egypt based on the happiness score over the years?
        
## Milestone 2:

### Milestone 2A:
    
    1. Feature Engineering. We added 3 new features to the datasets after we merged them (these features are attributes that can be inferred from the already given attributes). These are:
        a. Calculating the population density of each country in each year.
        b. Calculating the growth rate of life expectancy for each country every year.
        c. Calculating the population growth rate for each country every year.
    2. We then visualised these features and explored their added values. We also recorded our insights observed from these features.
   
### Milestone 2B:
    
    1. Creating a simplified ETL pipeline using Airflow.
        a. Extracting the data from a CSV file.
        b. Transforming the data by working on it (cleaning and tidying).
        c. Loading the data into new CSV files.
        
This was achieved by creating a DAG.py file, which can be found on the repository. This file is supposed to be placed in an airflowhome directory, inside the **dag** folder, from which an Airflow pipeline would be initiated. Moreover, there is another file that needs to be places inside the same **dag** folder, which is the ML.py file. This file is called by the DAG.py file in order to run a machine learning model (linear regression) in order to predict values in one of the datasets. This file is ran as a *BashOperator* so it need to be present in the same directory.
This directory must also include a **data** directory which should hold all the needed CSV files in the extract phase (the 3 datasets mentioned earlier).
The DAG is then initiated from the Airflow terminal.
Upon completion, the **data** directory would contain 5 new CSV files named:
    1. Country Dataset NEW
    2. Happiness Dataset NEW
    3. Life Expectancy NEW
    4. Merged Datasets
    5. Research Question 1 Answer
The two CSV files named "Merged Datasets" and "Research Question 1 Answer" are the CSV files of interest. They have the complete, merged datasets as well as the dataframe that would be used to answer our first research question respectively.

