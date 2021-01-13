import tweepy
import os
import string
import re
import pandas as pd
import nltk
from datetime import datetime
from textblob import TextBlob

# dateToday = datetime.date.today() + datetime.timedelta(hours=-2)
dateToday = datetime.utcnow().strftime('%Y-%m-%d')
print(dateToday)
# The folder relative to location on disk.
THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
# The location from which the data directory should be present.
my_file = os.path.join(os.path.normpath(os.path.join(
    THIS_FOLDER, os.pardir)), 'data/Tweets ' + str(dateToday) + '.csv')


def clean_text(text):
    text = re.sub(r'@\S+', '', text)  # --> remove mentions
    text = re.sub(r'http\S+', '', text)  # --> remove links
    text = text.encode("ascii", "ignore").decode() # --> remove non ASCII chars, like emojis
    text = text.replace(u"\ufffd", "?")
    text = re.sub(' +', ' ', text)
    return text


df_Tweets = pd.read_csv(my_file)

df_Tweets['Tweet'] = df_Tweets.apply(
    lambda row: clean_text(row['Tweet']), axis=1)
df_Tweets['Polarity'] = df_Tweets.apply(
    lambda row: TextBlob(row['Tweet']).sentiment.polarity, axis=1)
df_Tweets['Subjectivity'] = df_Tweets.apply(
    lambda row: TextBlob(row['Tweet']).sentiment.subjectivity, axis=1)

df_Tweets.to_csv(my_file, index=False)
