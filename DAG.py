# step 1 - import modules
import requests
import json
import numpy as np
import pandas as pd
# from sklearn.linear_model import LinearRegression
# from sklearn.preprocessing import MinMaxScaler

from datetime import datetime
from datetime import date
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG

# step 2 - define default args
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 25)
}

# step 3 - instantiate DAG
dag = DAG(
    'milestone_2B_dag',
    default_args=default_args,
    description='Do the data flow work of milestone 1',
    schedule_interval='@once',
)

# HELPER METHODS
def countriesWithoutXRecords(df, countries, c):
    result = []
    for country in countries:
        count = df[df['Country'] == country]['Country'].count()
        if count < c:
            result.append(country)
    return result

# Function to calculate the mean of *col* in a dataframe based on a condition passed along in the *compareCol* and *compareTo*.
def calculateMean(df, compareCol, compareTo, col):
    return df[df[compareCol] == compareTo][col].mean()

#  Function to replace values with *valueToReplace* in a dataframe based on a condition on the *compareCol* using a *compareTo* value in a specific *col*.
def replaceNaN(df, compareCol, compareTo, col, valueToReplace):
    df[col] = df.apply(lambda row: valueToReplace if (
        np.isnan(row[col]) and row[compareCol] == compareTo) else row[col], axis=1)

# We will replace all values that mostly don't make sense using these conditional values.
def replace(df, col, valueToReplace):
    df[col] = df.apply(lambda row: valueToReplace if (
        row[col] >= valueToReplace) else row[col], axis=1)


def applyCountryReplacement(df, col, country, compareTo, valueToReplace, right):
    if right:
        df.loc[(df.Country == country) & (
            df[col] > compareTo), col] = valueToReplace
    else:
        df.loc[(df.Country == country) & (
            df[col] < compareTo), col] = valueToReplace

# step 4 Define tasks
def extract_country_data(**kwargs):
    df_countryData = pd.read_csv('./data/250 Country Data.csv')
    return df_countryData.to_json()


def extract_happiness_data(**kwargs):
    df_happiness2015 = pd.read_csv('./data/Happiness_Dataset/2015.csv')
    df_happiness2015.rename(columns={
        'Happiness Rank': 'Happiness_Rank',
        'Happiness Score': 'Happiness_Score',
        'Standard Error': 'Standard_Error',
        'Economy (GDP per Capita)': 'Economy_GDP_per_Capita',
        'Health (Life Expectancy)': 'Health_Life_Expectancy',
        'Trust (Government Corruption)': 'Trust_Government_Corruption',
        'Dystopia Residual': 'Dystopia_Residual'
    }, inplace=True)
    df_happiness2016 = pd.read_csv('./data/Happiness_Dataset/2016.csv')
    df_happiness2016.rename(columns={
        'Happiness Rank': 'Happiness_Rank',
        'Happiness Score': 'Happiness_Score',
        'Lower Confidence Interval': 'Lower_Confidence_Interval',
        'Upper Confidence Interval': 'Upper_Confidence_Interval',
        'Economy (GDP per Capita)': 'Economy_GDP_per_Capita',
        'Health (Life Expectancy)': 'Health_Life_Expectancy',
        'Trust (Government Corruption)': 'Trust_Government_Corruption',
        'Dystopia Residual': 'Dystopia_Residual'
    }, inplace=True)
    df_happiness2017 = pd.read_csv('./data/Happiness_Dataset/2017.csv')
    df_happiness2017.rename(columns={
        'Happiness.Rank': 'Happiness_Rank',
        'Happiness.Score': 'Happiness_Score',
        'Whisker.low': 'Lower_Confidence_Interval',
        'Whisker.high': 'Upper_Confidence_Interval',
        'Economy..GDP.per.Capita.': 'Economy_GDP_per_Capita',
        'Health..Life.Expectancy.': 'Health_Life_Expectancy',
        'Trust..Government.Corruption.': 'Trust_Government_Corruption',
        'Dystopia.Residual': 'Dystopia_Residual'
    }, inplace=True)
    df_happiness2018 = pd.read_csv('./data/Happiness_Dataset/2018.csv')
    df_happiness2018.rename(columns={
        'Overall rank': 'Happiness_Rank',
        'Country or region': 'Country',
        'Score': 'Happiness_Score',
        'GDP per capita': 'Economy_GDP_per_Capita',
        'Healthy life expectancy': 'Health_Life_Expectancy',
        'Freedom to make life choices': 'Freedom',
        'Perceptions of corruption': 'Trust_Government_Corruption',
        'Social support': 'Family'
    }, inplace=True)
    df_happiness2019 = pd.read_csv('./data/Happiness_Dataset/2015.csv')
    df_happiness2019.rename(columns={
        'Happiness Rank': 'Happiness_Rank',
        'Happiness Score': 'Happiness_Score',
        'Standard Error': 'Standard_Error',
        'Economy (GDP per Capita)': 'Economy_GDP_per_Capita',
        'Health (Life Expectancy)': 'Health_Life_Expectancy',
        'Trust (Government Corruption)': 'Trust_Government_Corruption',
        'Dystopia Residual': 'Dystopia_Residual'
    }, inplace=True)
    df_happiness2015["Year"] = 2015
    df_happiness2016["Year"] = 2016
    df_happiness2017["Year"] = 2017
    df_happiness2018["Year"] = 2018
    df_happiness2019["Year"] = 2019
    df_happiness = pd.concat([df_happiness2015, df_happiness2016,
                              df_happiness2017, df_happiness2018, df_happiness2019])
    df_happiness.sort_values(by=['Country', 'Year'], inplace=True)

    columnsIn2015Happiness = np.array(df_happiness2015.columns)
    columnsIn2016Happiness = np.array(df_happiness2016.columns)
    columnsIn2017Happiness = np.array(df_happiness2017.columns)
    columnsIn2018Happiness = np.array(df_happiness2018.columns)
    columnsIn2019Happiness = np.array(df_happiness2019.columns)
    columnsInAllHappinessDatasets = np.intersect1d(
        np.intersect1d(
            np.intersect1d(
                np.intersect1d(
                    columnsIn2015Happiness,
                    columnsIn2016Happiness
                ),
                columnsIn2017Happiness
            ),
            columnsIn2018Happiness
        ),
        columnsIn2019Happiness
    )
    df_happiness = df_happiness[columnsInAllHappinessDatasets]
    df_happiness.reset_index(drop=True, inplace=True)
    return df_happiness.to_json()


def extract_lifeExpectancy_data(**kwargs):
    df_lifeExpectancy = pd.read_csv('./data/Life Expectancy Data.csv')
    df_lifeExpectancy.set_index(['Country', 'Year'])
    df_lifeExpectancy.rename(columns=lambda x: x.strip(), inplace=True)
    df_lifeExpectancy.columns = [name.capitalize()
                                 for name in df_lifeExpectancy.columns]
    df_lifeExpectancy.rename(columns={
        'Life expectancy': 'Life_Expectancy',
        'Adult mortality': 'Adult_Mortality',
        'Infant deaths': 'Infant_Deaths',
        'Percentage expenditure': 'Percentage_Expenditure',
        'Hepatitis b': 'Hepatitis_B',
        'Bmi': 'BMI',
        'Under-five deaths': 'Under-five_Deaths',
        'Total expenditure': 'Total_Expenditure',
        'Hiv/aids': 'HIV/AIDS',
        'Gdp': 'GDP',
        'Thinness  1-19 years': 'Thinness_10-19_Years',
        'Thinness 5-9 years': 'Thinness_5-9_Years',
        'Income composition of resources': 'Income_Composition_of_Resources'
    }, inplace=True)
    return df_lifeExpectancy.to_json()


def work_on_country_data(**context):
    body = context['task_instance'].xcom_pull(
        task_ids='extract_country_data')
    df_countryData = pd.DataFrame(json.loads(body))
    df_countryData.rename(columns={'name': 'Country'}, inplace=True)
    df_countryData.drop('Unnamed: 0', inplace=True, axis=1)
    df_countryData.rename(columns={
        'region': 'Region',
        'subregion': 'Subregion',
        'population': 'Population',
        'area': 'Area',
        'gini': 'Gini',
        'Real Growth Rating(%)': 'Real Growth Rating',
        'Literacy Rate(%)': 'Literacy Rate',
        'Inflation(%)': 'Inflation',
        'Unemployement(%)': 'Unemployment'
    }, inplace=True)
    df_countryData.to_csv("data/Country Dataset NEW.csv", index=False)


def work_on_happiness_data(**context):
    body = context['task_instance'].xcom_pull(
        task_ids='extract_happiness_data')
    df_happiness = pd.DataFrame(json.loads(body))
    df_happiness['Country'] = df_happiness['Country'].replace(
        {'&': 'and'}, regex=True)
    df_happiness['Country'] = df_happiness['Country'].replace({
        'Northern Cyprus': 'North Cyprus',
        'Somaliland region': 'Somaliland Region',
        'Taiwan Province of China': 'Taiwan',
        'Hong Kong S.A.R., China': 'Hong Kong'
    })
    countries = df_happiness.groupby('Country').groups.keys()
    countriesWithout5Records = countriesWithoutXRecords(
        df_happiness, countries, 5)
    df_happiness = df_happiness.loc[(
        ~df_happiness['Country'].isin(countriesWithout5Records)), :]
    df_happiness.to_csv("data/Happiness Dataset NEW.csv", index=False)


def work_on_lifeExpectancy_data(**context):
    body = context['task_instance'].xcom_pull(
        task_ids='extract_lifeExpectancy_data')
    df_lifeExpectancy = pd.DataFrame(json.loads(body))
    countriesInLifeExpectancy = df_lifeExpectancy.groupby(
        'Country').groups.keys()
    countriesWithout16Records = countriesWithoutXRecords(
        df_lifeExpectancy, countriesInLifeExpectancy, 16)
    df_lifeExpectancy = df_lifeExpectancy.loc[(
        ~df_lifeExpectancy['Country'].isin(countriesWithout16Records)), :]
    df_lifeExpectancy = df_lifeExpectancy[['Country', 'Year', 'Status', 'Life_Expectancy', 'Adult_Mortality',
                                           'Infant_Deaths', 'Measles', 'Polio', 'Diphtheria', 'HIV/AIDS', 'Thinness_5-9_Years', 'Thinness_10-19_Years']]
    df_lifeExpectancy.to_csv('data/Life Expectancy Dataset NEW.csv', index=False)


def impute_lifeExpectancy_data():
    df_lifeExpectancy = pd.read_csv('./data/Life Expectancy Dataset NEW.csv')
    averageOfDevelopedPolio = calculateMean(
        df_lifeExpectancy, 'Status', 'Developed', 'Polio')
    averageOfDevelopingPolio = calculateMean(
        df_lifeExpectancy, 'Status', 'Developing', 'Polio')

    averageOfDevelopedDiph = calculateMean(
        df_lifeExpectancy, 'Status', 'Developed', 'Diphtheria')
    averageOfDevelopingDiph = calculateMean(
        df_lifeExpectancy, 'Status', 'Developing', 'Diphtheria')

    averageOfDevelopedThinness = calculateMean(
        df_lifeExpectancy, 'Status', 'Developed', 'Thinness_5-9_Years')
    averageOfDevelopingThinness = calculateMean(
        df_lifeExpectancy, 'Status', 'Developing', 'Thinness_5-9_Years')

    averageOfDevelopedThinness2 = calculateMean(
        df_lifeExpectancy, 'Status', 'Developed', 'Thinness_10-19_Years')
    averageOfDevelopingThinness2 = calculateMean(
        df_lifeExpectancy, 'Status', 'Developing', 'Thinness_10-19_Years')

    replaceNaN(df_lifeExpectancy, 'Status', 'Developed',
               'Polio', averageOfDevelopedPolio)
    replaceNaN(df_lifeExpectancy, 'Status', 'Developing',
               'Polio', averageOfDevelopingPolio)

    replaceNaN(df_lifeExpectancy, 'Status', 'Developed',
               'Diphtheria', averageOfDevelopedDiph)
    replaceNaN(df_lifeExpectancy, 'Status', 'Developing',
               'Diphtheria', averageOfDevelopingDiph)

    replaceNaN(df_lifeExpectancy, 'Status', 'Developed',
               'Thinness_5-9_Years', averageOfDevelopedThinness)
    replaceNaN(df_lifeExpectancy, 'Status', 'Developing',
               'Thinness_5-9_Years', averageOfDevelopingThinness)

    replaceNaN(df_lifeExpectancy, 'Status', 'Developed',
               'Thinness_10-19_Years', averageOfDevelopedThinness2)
    replaceNaN(df_lifeExpectancy, 'Status', 'Developing',
               'Thinness_10-19_Years', averageOfDevelopingThinness2)

    Q1 = df_lifeExpectancy.quantile(0.25)
    Q3 = df_lifeExpectancy.quantile(0.75)
    IQR = Q3 - Q1
    maxValueOfMeasles = (IQR['Measles'] * 1.5) + Q3['Measles']
    maxValueOfInfantDeaths = (IQR['Infant_Deaths'] * 1.5) + Q3['Infant_Deaths']

    maxValueSatisfyingConditionMeasles = df_lifeExpectancy[~(
        df_lifeExpectancy['Measles'] > maxValueOfMeasles)]['Measles'].max()
    maxValueSatisfyingConditionInfantDeaths = df_lifeExpectancy[~(
        df_lifeExpectancy['Infant_Deaths'] > maxValueOfInfantDeaths)]['Infant_Deaths'].max()

    replace(df_lifeExpectancy, 'Measles', maxValueSatisfyingConditionMeasles)
    replace(df_lifeExpectancy, 'Infant_Deaths',
            maxValueSatisfyingConditionInfantDeaths)

    countriesInLifeExpectancy = df_lifeExpectancy.groupby(
        'Country').groups.keys()
    # Removing outliers from the left and from the right.
    for country in countriesInLifeExpectancy:
        for col in df_lifeExpectancy.columns[4:]:
            maxValue = (IQR[col] * 1.5) + Q3[col]
            minValue = Q1[col] - (IQR[col] * 1.5)
            mean = calculateMean(df_lifeExpectancy, 'Country', country, col)
            applyCountryReplacement(
                df_lifeExpectancy, col, country, maxValue, mean, True)
            applyCountryReplacement(
                df_lifeExpectancy, col, country, minValue, mean, False)
    df_lifeExpectancy.to_csv(
        "data/Life Expectancy Dataset NEW.csv", index=False)


def answer_research_q1():
    df_mergedThree = pd.read_csv('./data/Merged Datasets.csv')
    regions = df_mergedThree.groupby('Region').groups.keys()
    lifeExpectancyMeanByRegion = []
    happinessMeanByRegion = []
    regionArea = []
    for region in regions:
        meanLE = df_mergedThree[df_mergedThree['Region']
                                == region]['Life_Expectancy'].mean()
        meanH = df_mergedThree[df_mergedThree['Region']
                               == region]['Happiness_Score'].mean()
        sumArea = df_mergedThree[df_mergedThree['Region']
                                 == region]['Area'].sum()
        sumArea = sumArea/5
        lifeExpectancyMeanByRegion.append(meanLE)
        happinessMeanByRegion.append(meanH)
        regionArea.append(sumArea)

    df_q1 = pd.DataFrame({'Region': regions, 'Life_Expectancy': lifeExpectancyMeanByRegion,
                          'Happiness_Score': happinessMeanByRegion, 'Area': regionArea})
    df_q1.to_csv('data/Research Question 1 Answer.csv', index=False)


extractCountryData = PythonOperator(
    task_id='extract_country_data',
    provide_context=True,
    python_callable=extract_country_data,
    dag=dag,
)

workOnCountyData = PythonOperator(
    task_id='work_on_country_data',
    provide_context=True,
    python_callable=work_on_country_data,
    dag=dag,
)

extractHappinessData = PythonOperator(
    task_id='extract_happiness_data',
    provide_context=True,
    python_callable=extract_happiness_data,
    dag=dag,
)

workOnHappinessData = PythonOperator(
    task_id='work_on_happiness_data',
    provide_context=True,
    python_callable=work_on_happiness_data,
    dag=dag,
)

extractLifeExpectancyData = PythonOperator(
    task_id='extract_lifeExpectancy_data',
    provide_context=True,
    python_callable=extract_lifeExpectancy_data,
    dag=dag,
)

workOnLifeExpectancyData = PythonOperator(
    task_id='work_on_lifeExpectancy_data',
    provide_context=True,
    python_callable=work_on_lifeExpectancy_data,
    dag=dag,
)

imputeLifeExpectancy = PythonOperator(
    task_id='impute_lifeExpectancy_data',
    provide_context=True,
    python_callable=impute_lifeExpectancy_data,
    dag=dag,
)

applyLinearRegression = BashOperator(
    task_id='apply_linear_regression',
    bash_command='python ../../../../c/Users/gasse/airflowhome/dags/ML.py',
    dag=dag,
)

joinThreeDatasets = BashOperator(
    task_id='joining_three_datasets',
    bash_command='python ../../../../c/Users/gasse/airflowhome/dags/DatasetsJoin.py',
    dag=dag,
)

answerResearchQuestion1 = PythonOperator(
    task_id='answer_research_q1',
    provide_context=True,
    python_callable=answer_research_q1,
    dag=dag,
)

# step 5 - define dependencies
extractCountryData >> workOnCountyData >> extractHappinessData >> workOnHappinessData >> extractLifeExpectancyData >> workOnLifeExpectancyData >> imputeLifeExpectancy >> applyLinearRegression >> joinThreeDatasets >> answerResearchQuestion1
