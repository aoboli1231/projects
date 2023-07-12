# COMP90024 Project 2 2023
# Contributor
# Aobo Li              1172339
# Pavith Samarakoon    1297058
# Zhihao Liang         1367102
# Jiqiang Chen         1171420
# Yijun Liu            1132416


from mastodon import Mastodon, MastodonNotFoundError, MastodonRatelimitError, StreamListener, MastodonNetworkError
import csv, os, time, json
import re
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from textblob import TextBlob
import datetime
import couchdb
import nltk
from nltk.stem import WordNetLemmatizer
import time

# Download the required NLTK resources
nltk.download('punkt')
nltk.download('wordnet')

# pip install googletrans==3.1.0a0
# pip install googletrans doesn't work, will have translate issue
from googletrans import Translator
import warnings

translator = Translator()
lemmatizer = WordNetLemmatizer()

username = 'admin'
password = r'1dTY1!PWM2'
ip = '45.113.234.176'
url = 'http://{}:{}@{}:5984/'.format(username, password, ip)
 
# Connect to CouchDB
# username:password@localhost:5984
couch = couchdb.Server(url)
db_name = 'mastodon_data_v3'
if db_name not in couch:
    db = couch.create(db_name)
else:
    db = couch[db_name]


m = Mastodon(
    api_base_url='https://mastodon.au',
    access_token='Ji9hWsGcTpCyzly8ibuNyCvcimle0P6SMQDDzTdvJjk'
)
class Listener(StreamListener):
    def on_update(self, status):

        # Parse the HTML content using BeautifulSoup, toots are formatted in html form
        # some toots may raise MarkupResemblesLocatorWarning, skip these toots as they might create problems
        with warnings.catch_warnings(record=True) as w:
            soup = BeautifulSoup(status["content"], 'html.parser')
            for warning in w:
                if isinstance(warning.message, MarkupResemblesLocatorWarning):
                    print(status["content"])
                    return
        # if there are hashtags in string, replace with whitespace
        content = soup.get_text().replace("#", " ")
        
        
        # check if content is empty, if it is, skip this toot
        if len(content) == 0:
            return
        # get language of toot
        language = status['language']
        
        translated_content = None
        # check if language is in english, if not, then textblob can't return a valid sentiment score
        # use google translate to translate this sentence so sentiment analysis will work
        # use a new variable data to be used for lemmatisation
        if language != "en":
            translated_content = translator.translate(content, dest='en').text
            data = translated_content
        else:
            data = content

        # Analyze the sentiment of the content using TextBlob
        blob = TextBlob(data)
        sentiment_score = blob.sentiment.polarity
        
        # get date of this toot
        # select only year and month and hour of toot
        status_date = status['created_at']
        status_date_str = status_date.strftime('%Y-%m-%d')[:-3]
        status_hour_str = status_date.hour
        # tokenise word for lemmatisation
        words = nltk.word_tokenize(data)
        # Lemmatize each word in the list of words
        lemmatized_words = [lemmatizer.lemmatize(word) for word in words]
        # Join the lemmatized words back into a string, lower the case os string
        lemmatized_content = " ".join(lemmatized_words).lower()
        
        # Search for keywords of Russia and Ukraine war
        # try to only contain words that are Russian Ukraine war related
        RU_keywords = ['russia', 'ukraine','russian', 'ukrainian', 'war', 'invasion', 'troop', 'crimea', 'donbass', 'luhansk', 'sevastopol', 'maidan', 'annexation', 'occupation', 'shelling', 'nato', 'wagner', 'belgorod', 'putin', 'zelensky']
        
        RUwar = False
        RUwar_word_count = 0
        # check if russia or ukraine is included in the sentence, otherwise it could be get data containing other keywords but not relate to Russian Ukrainian war
        if re.search(r'\b(russia|ukraine)\b', lemmatized_content):
            # if there are more than 2 of keywords occur in a toot, we'll consider it as war related
            for keyword in RU_keywords:
                # use set for findall to check if more than 1 word occured in the keyword list, ignore cases
                # only exact match of the word is considered
                RUwar_word_count += len(set(re.findall(r'\b{}\b'.format(keyword), lemmatized_content)))
                if RUwar_word_count>1:
                    RUwar = True
                    break
        
        # search for keyword of rental related toots
        # try to only contain words that are Russian Ukraine war related
        rental_keywords = ['rent', 'rental', 'lease', 'tenant', 'apartment', 'house', 'property', 'real estate', 'contract', 'utilities']
        
        # if there are more than 2 of keywords occur in a toot, we'll consider it as rental related
        # only exact match of the word is considered
        rental = False
        rental_word_count = 0
        for keyword in rental_keywords:
            # use set for findall to check if more than 1 word occured in the keyword list
            rental_word_count += len(set(re.findall(r'\b{}\b'.format(keyword), lemmatized_content)))
            if rental_word_count > 1:
                rental = True
                break
        # Process the data as desired
        processed_data = {
            'date': status_date_str,
            'hour':status_hour_str,
            'content': content,
            'translated_content': translated_content,
            'language': language,
            'sentiment': sentiment_score,
            'RUwar': RUwar,
            'rental': rental
        }

        # Write the processed data to couchdb
        db.save(processed_data)

while True:
    try:
        listener = Listener()
        m.stream_public(listener)
    except MastodonNetworkError as e:
        print(f"Caught MastodonNetworkError: {e}")
        # sleep for a short period of time before attempting to reconnect
        time.sleep(15)
        # re-initiate mastodon api in case of an error
        m = Mastodon(
            api_base_url='https://mastodon.au',
            access_token='Ji9hWsGcTpCyzly8ibuNyCvcimle0P6SMQDDzTdvJjk'
            )
    except Exception as e:
            print(f"Caught Exception: {e}")
            time.sleep(15)
            # re-initiate mastodon api in case of an error
            m = Mastodon(
                api_base_url='https://mastodon.au',
                access_token='Ji9hWsGcTpCyzly8ibuNyCvcimle0P6SMQDDzTdvJjk'
                )
