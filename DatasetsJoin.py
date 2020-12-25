#!/usr/bin/python2.4
# -*- coding: utf-8 -*-
# step 1 - import modules
import json
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import MinMaxScaler


def getCountriesList(df):
    return df.groupby(['Country']).groups.keys()


df_lifeExpectancy = pd.read_csv(
    '../../../../c/Users/gasse/airflowhome/data/Life Expectancy Dataset NEW.csv')
df_happiness = pd.read_csv(
    '../../../../c/Users/gasse/airflowhome/data/Happiness Dataset NEW.csv')
df_countryData = pd.read_csv(
    '../../../../c/Users/gasse/airflowhome/data/Country Dataset NEW.csv')

# df_lifeExpectancy = pd.read_csv(
#     '../data/Life Expectancy Dataset NEW.csv')
# df_happiness = pd.read_csv(
#     '../data/Happiness Dataset NEW.csv')
# df_countryData = pd.read_csv(
#     '../data/Country Dataset NEW.csv')
# df_lifeExpectancy = pd.read_csv('../data/Life Expectancy Dataset NEW.csv')
df_lifeExpectancy['Country']
df_lifeExpectancy['Country'] = df_lifeExpectancy['Country'].replace({
    'Iran (Islamic Republic of)': 'Iran',
    'Czechia': 'Czech Republic',
    'Democratic Republic of the Congo': 'Congo (Kinshasa)',
    'Congo': 'Congo (Brazzaville)',
    'United Republic of Tanzania': 'Tanzania',
    'Russian Federation': 'Russia',
    'United States of America': 'United States',
    'Venezuela (Bolivarian Republic of)': 'Venezuela',
    "Côte d'Ivoire": 'Ivory Coast',
    'Viet Nam': 'Vietnam',
    'Republic of Korea': 'South Korea',
    'Syrian Arab Republic': 'Syria',
    "Lao People's Democratic Republic": 'Laos',
    'Republic of Moldova': 'Moldova',
    'United Kingdom of Great Britain and Northern Ireland': 'United Kingdom',
    'Bolivia (Plurinational State of)': 'Bolivia',
    'The former Yugoslav republic of Macedonia': 'Macedonia',
    'Czechia': 'Czech Republic'
})
countriesListJoined_lifeExpectancy_happiness = set(
    getCountriesList(df_lifeExpectancy)) & set(getCountriesList(df_happiness))
df_countryData['Country'] = df_countryData['Country'].replace({
    'Iran (Islamic Republic of)': 'Iran',
    'Tanzania, United Republic of': 'Tanzania',
    'Congo': 'Congo (Brazzaville)',
    'Congo (Democratic Republic of the)': 'Congo (Kinshasa)',
    'Russian Federation': 'Russia',
    'United States of America': 'United States',
    'Venezuela (Bolivarian Republic of)': 'Venezuela',
    "Côte d'Ivoire": 'Ivory Coast',
    'Viet Nam': 'Vietnam',
    'Korea (Republic of)': 'South Korea',
    'Syrian Arab Republic': 'Syria',
    "Lao People's Democratic Republic": 'Laos',
    'Moldova (Republic of)': 'Moldova',
    'United Kingdom of Great Britain and Northern Ireland': 'United Kingdom',
    'Bolivia (Plurinational State of)': 'Bolivia',
    'Macedonia (the former Yugoslav Republic of)': 'Macedonia'
})
countriesList_joinedThree = set(getCountriesList(df_countryData)) & set(
    countriesListJoined_lifeExpectancy_happiness)
df_countryData = df_countryData.loc[(
    df_countryData['Country'].isin(countriesList_joinedThree)), :]
df_happiness = df_happiness.loc[(
    df_happiness['Country'].isin(countriesList_joinedThree)), :]
df_lifeExpectancy = df_lifeExpectancy.loc[(
    df_lifeExpectancy['Country'].isin(countriesList_joinedThree)), :]
df_merged_expectancyHappiness = pd.merge(df_lifeExpectancy, df_happiness, on=[
                                         'Country', 'Year'], how='inner')
df_mergedThree = pd.merge(df_merged_expectancyHappiness,
                          df_countryData, on='Country', how='inner')

df_mergedThree.to_csv(
    "../../../../c/Users/gasse/airflowhome/data/Merged Datasets.csv", index=False)
# df_lifeExpectancy.to_csv('../data/batates.csv', index=False)
