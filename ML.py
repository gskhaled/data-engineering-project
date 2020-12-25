# step 1 - import modules
import json
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import MinMaxScaler

df_lifeExpectancy = pd.read_csv(
    '../../../../c/Users/gasse/airflowhome/data/Life Expectancy Dataset NEW.csv')
# df_lifeExpectancy = pd.read_csv('../data/Life Expectancy Dataset NEW.csv')
model = LinearRegression()
years = df_lifeExpectancy.groupby(['Year']).groups.keys()
years = np.flip(np.array([years]).reshape(-1, 1))
allCountries = df_lifeExpectancy.groupby(['Country']).groups.keys()
# allCountries = [allCountries]
years_predict = np.array([2016, 2017, 2018, 2019]).reshape(-1, 1)
dicData = {}  # Dictionary to include the records for these years.
i = 0
for country in allCountries:
    dicCountry = {}  # Dictionary to hold the records for a single country.
    for columnName, columnData in df_lifeExpectancy.iteritems():
        if(columnName == 'Country'):
            dicCountry[columnName] = [country] * 4
            continue
        if(columnName == 'Status'):
            dicCountry[columnName] = [
                df_lifeExpectancy[df_lifeExpectancy['Country'] == country][columnName].tolist()[0]] * 4
            continue
        if(columnName == 'Year'):
            dicCountry[columnName] = [2016, 2017, 2018, 2019]
            continue
        x_train = np.array(
            df_lifeExpectancy[df_lifeExpectancy['Country'] == country][columnName]).reshape(-1, 1)
        model.fit(years, x_train)
        x_predict = model.predict(years_predict)
        dicCountry[columnName] = x_predict.flatten().tolist()
    i += 1
    dicData[country] = dicCountry

for country in dicData.keys():
    entry2016 = {}
    entry2017 = {}
    entry2018 = {}
    entry2019 = {}
    for col in dicData[country]:
        entry2016[col] = dicData[country][col][0]
        entry2017[col] = dicData[country][col][1]
        entry2018[col] = dicData[country][col][2]
        entry2019[col] = dicData[country][col][3]
    entries = pd.DataFrame([entry2016, entry2017, entry2018, entry2019])
    df_lifeExpectancy = df_lifeExpectancy.append(entries)

df_lifeExpectancy.sort_values(by=['Country', 'Year'], inplace=True)
df_lifeExpectancy = df_lifeExpectancy.reset_index(drop=True)
df_lifeExpectancy = df_lifeExpectancy.loc[df_lifeExpectancy['Year'] >= 2015, :]
# df_lifeExpectancy.drop(['Unnamed: 0', 'Unnamed: 0.1', 'Unnamed: 0.1.1'], axis=1, inplace=True)
df_lifeExpectancy.to_csv(
    "../../../../c/Users/gasse/airflowhome/data/Life Expectancy Dataset NEW.csv", index=False)
# df_lifeExpectancy.to_csv('data/batates.csv', index=False)
