import os
import string
import re
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler

# dateToday = datetime.date.today() + datetime.timedelta(hours=-2)
dateToday = datetime.utcnow().strftime('%Y-%m-%d')
# The folder relative to location on disk.
THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
# The location from which the data directory should be present.
average_file = os.path.join(os.path.normpath(os.path.join(
    THIS_FOLDER, os.pardir)), 'data/Tweets Average.csv')
tweets_file = os.path.join(os.path.normpath(os.path.join(
    THIS_FOLDER, os.pardir)), 'data/Tweets ' + str(dateToday) + '.csv')
happiness_dataset = os.path.join(os.path.normpath(os.path.join(
    THIS_FOLDER, os.pardir)), 'data/Happiness Dataset NEW.csv')

if os.path.isfile(average_file):
    df_Average = pd.read_csv(average_file)
else:
    df_Average = pd.DataFrame({
        'Country': [],
        'Date': [],
        'Average_Polarity': [],
        'Average_Subjectivity': [],
        'Comparison_To_Happiness_Score': []
    })

df_Tweets = pd.read_csv(tweets_file)
df_happiness = pd.read_csv(happiness_dataset)

df_happiness = df_happiness[df_happiness['Year'] == 2019]
df_happiness['Happiness_Score'] = MinMaxScaler(feature_range=(-1, 1)).fit_transform(df_happiness[['Happiness_Score']])
egyptHappinessScaledAvg = df_happiness[df_happiness['Country'] == 'Egypt']['Happiness_Score'].mean()
canadaHappinessScaledAvg = df_happiness[df_happiness['Country'] == 'Canada']['Happiness_Score'].mean()

def isHappier(value1, value2):
    return "Score is happier" if value1 > value2 else "Score is not happier"

averagePolarityEgypt = df_Tweets[df_Tweets['Country'] == 'Egypt']['Polarity'].mean()
averagePolarityCanada = df_Tweets[df_Tweets['Country'] == 'Canada']['Polarity'].mean()
averageSubjectivityEgypt = df_Tweets[df_Tweets['Country'] == 'Egypt']['Subjectivity'].mean()
averageSubjectivityCanada = df_Tweets[df_Tweets['Country'] == 'Canada']['Subjectivity'].mean()

time = datetime.utcnow()
df_ToAppend = pd.DataFrame({
    'Country': ['Egypt', 'Canada'],
    'Date': [time, time],
    'Average_Polarity': [averagePolarityEgypt, averagePolarityCanada],
    'Average_Subjectivity': [averageSubjectivityEgypt, averageSubjectivityCanada],
    'Comparison_To_Happiness_Score': [isHappier(averagePolarityEgypt, egyptHappinessScaledAvg), isHappier(averagePolarityCanada, canadaHappinessScaledAvg)]
})

df_Average = df_Average.append(df_ToAppend)

df_Average.to_csv(average_file, index=False)