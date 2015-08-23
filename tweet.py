import threading
import os
import tweepy
import sqlite3
import datetime
from config import *

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)

def set_interval(func, sec):
    """
    Execute func every sec seconds.
    """
    def func_wrapper():
        set_interval(func, sec)
        func()
    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t

def initialize_nouns_db():
    """
    Dump the contents of nouns.txt into a SQLite database
    """
    db = sqlite3.connect("nouns.db")
    cursor = db.cursor()
    cursor.execute('''CREATE TABLE nouns (noun text, tweeted integer, tweet_datetime text)''')
    with open("nouns.txt") as nouns:
        for noun in nouns:
            noun = noun.strip()
            query_noun = [noun]
            cursor.execute('''INSERT INTO nouns VALUES (?, 0, 'none')''', query_noun)
            print("%s has been inserted into nouns.db" % noun)
    db.commit()
    db.close()

def fetch_word():
    """
    Find a noun from the list, then return and delete it.
    """
    db = sqlite3.connect("nouns.db")
    cursor = db.cursor()
    cursor.execute('''SELECT noun FROM nouns WHERE tweeted = 0 ORDER BY RANDOM() LIMIT 1;''')
    noun = cursor.fetchone()[0]
    print(noun)
    cursor.execute('''UPDATE nouns SET tweeted=1, tweet_datetime=? WHERE noun==?;''', 
            (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), noun))
    db.commit()
    db.close()
    return noun

def send_tweet(noun):
    """
    Submits a tweet when given a noun.
    """
    tweet = "Data-driven %s" % noun
    api.update_status(status = tweet)
    print("Successfully posted tweet: ", tweet)

def find_noun_and_tweet():
    noun = find_noun()
    send_tweet(noun)

if __name__ == "__main__":
    if not os.path.isfile("nouns.db"):
       initialize_nouns_db()
    
    set_interval(find_noun_and_tweet, 600)
