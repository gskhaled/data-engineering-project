import tweepy
import time
import os
import pandas as pd
import json
from datetime import datetime
import TwitterConfig as config

dateToday = datetime.utcnow().strftime('%Y-%m-%d')
print(dateToday)

twitter = tweepy.OAuthHandler(
    config.twitter['consumer_key'], config.twitter['consumer_secret'])
twitter.set_access_token(config.twitter['key'], config.twitter['secret'])

api = tweepy.API(twitter)
try:
    api.verify_credentials()
    print("Authenticated Successfully")
except:
    print("ERROR, TWITTER NOT AUTHENTICATED!")

tweets_egypt = tweepy.Cursor(api.search, q="-filter:retweets", result_type='recent', geocode='26.820553,30.802498,500km',
                       since=dateToday, lang='en', include_entities='false', tweet_mode='extended').items(50)
tweets_full_text = []
users = []
dates = []
for tweet in tweets_egypt:
    tweets_full_text.append(tweet.full_text)
    users.append(tweet._json['user']['screen_name'])
    dates.append(tweet._json['created_at'])
country_egypt = ['Egypt' for i in range(50)]

tweets_canada = tweepy.Cursor(api.search, q="-filter:retweets", result_type='recent', geocode='59.763592,-112.114943,1000km',
                       since=dateToday, lang='en', include_entities='false', tweet_mode='extended').items(50)
for tweet in tweets_canada:
    tweets_full_text.append(tweet.full_text)
    users.append(tweet._json['user']['screen_name'])
    dates.append(tweet._json['created_at'])
country_canada = ['Canada' for i in range(50)]

df_Tweets = pd.DataFrame(
    {'Country': country_egypt + country_canada, 'Screen_name': users, 'Tweet': tweets_full_text, 'Date': dates})

# The folder relative to location on disk.
THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
# The location from which the data directory should be present.
my_file = os.path.join(os.path.normpath(os.path.join(
    THIS_FOLDER, os.pardir)), 'data/Tweets ' + str(dateToday) + '.csv')

df_Tweets.to_csv(my_file, index=False, encoding='utf-8-sig')
