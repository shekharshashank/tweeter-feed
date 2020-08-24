#!/usr/bin/env python
# coding: utf-8

# In[36]:


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from  kafka import  KafkaClient, KafkaProducer
import json


# In[37]:


access_token = ""
access_token_secret =  ""
consumer_key =  ""
consumer_secret =  ""


# In[98]:


class StdOutListener(StreamListener):
    def on_status(self, status):        
#         if status.retweeted:
#             return
#         print(status.text)
        if hasattr(status,'retweeted_status')==False:
            return 
#         print(status.retweeted_status)
        id_str = status.id_str
        created = status.created_at
        text = status.text
        fav = status.favorite_count
        name = status.user.screen_name
        description = status.user.description
        loc = status.user.location
        user_created = status.user.created_at
        followers = status.user.followers_count
        country=None
#         print(status.place)
        if status.place:
            country = status.place.country
            place_name= status.place.name
            place_type= status.place.place_type
            
        print(status.user.screen_name.encode('utf-8'))

#         producer.send("quickstart-events", (id_str+","+str(created)+","+str(fav)+"."+str(name)+","+str(loc)+","+str(followers)+","+str(text)).encode('utf-8'))
        producer.send("quickstart-events", key=status.user.screen_name.encode('utf-8'),value=str(status.retweeted_status.retweet_count).encode('utf-8'))
#         print(id_str+","+str(created)+","+str(fav)+"."+str(name)+","+str(loc)+","+str(followers)+","+str(text))
    def on_error(self, status):
        print (status)
        return True

kafka = KafkaClient(hosts="localhost:9092")
# print(kafka.topics)
producer = KafkaProducer(bootstrap_servers="localhost:9092")
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["Sushant","SSR","RAJPUT"])


# In[ ]:




