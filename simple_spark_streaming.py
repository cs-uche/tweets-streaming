#!/usr/bin/env python3

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from geopy.geocoders import Nominatim
from textblob import TextBlob
from elasticsearch import Elasticsearch

TCP_IP = 'localhost'
TCP_PORT = 9001


def processTweet(tweet):    
    #connect to elastic
    es = Elasticsearch([{'host' : 'localhost', 'port' : 9200}])
    # mapping dictionary 
    '''mappings = {
        "properties": {
          "location": {
            "type": "geo_point"
          }
        }
    }'''
    tweetData = tweet.split("::")

    if len(tweetData) > 1:
       
        text = tweetData[1]
        rawLocation = tweetData[0]

        # (i) Apply Sentiment analysis in "text"
        ## Using Textblob
        if float(TextBlob(text).sentiment.polarity) > 0.3:
            sentiment = "Positive"
        elif float(TextBlob(text).sentiment.polarity) < -0.3:
            sentiment = "Negative"
        else:
            sentiment = "Neutral"
# (ii) Get geolocation (state, country, lat, lon, etc...) from rawLocation
        #start geolocator 
        geolocator = Nominatim(user_agent="BigData_Uche")
        try:
            location = geolocator.geocode(tweetData[0],addressdetails = True)
            lat = location.raw['lat']
            lon = location.raw['lon']
            state = location.raw['address']['state']
            country = location.raw['address']['country']
            geopoint = doc['location'].value
        except:
            lat = lon = state = country = None
        

        # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!)
        if lat !=None and lon !=None and sentiment !=None:
            doc = {"lat":lat,"lon":lon, "state":state, "country":country, "Sentiment":sentiment}
            #indexing
            es.index(index='tweetSentiment', doc_type='default', body=doc)
            '''es.indices.create(index='geo-index', body=mappings)
            es_entries['properties']={'location':str(lon)+","+str(lat)}
            es.index(index="geo-index", doc_type="doc", body=es_entries)'''


# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)


dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))


ssc.start()
ssc.awaitTermination()