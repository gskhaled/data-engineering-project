# step 1 - import modules
import os
import json
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import MinMaxScaler
# The folder relative to location on disk.
THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
# The location from which the data directory should be present.
my_file = os.path.join(os.path.normpath(os.path.join(
    THIS_FOLDER, os.pardir)), 'data/Life Expectancy Dataset New.csv')
# Importing the data frame.
df_lifeExpectancy = pd.read_csv(my_file)

model = LinearRegression()
# Years already in the dataframe, which is used for training.
years = df_lifeExpectancy.groupby(['Year']).groups.keys()
years = np.flip(np.array([years]).reshape(-1, 1))
# List of all countries in dataframe.
allCountries = df_lifeExpectancy.groupby(['Country']).groups.keys()
# The years that will be used to predict upon.
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

# Resorting the values in the dataframe to group by country and year.
df_lifeExpectancy.sort_values(by=['Country', 'Year'], inplace=True)
df_lifeExpectancy = df_lifeExpectancy.reset_index(drop=True)
# Removing the values for years that are less than 2015.
df_lifeExpectancy = df_lifeExpectancy.loc[df_lifeExpectancy['Year'] >= 2015, :]
# Saving in the same location on disk.
df_lifeExpectancy.to_csv(my_file, index=False)
