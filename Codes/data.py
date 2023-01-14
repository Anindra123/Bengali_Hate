# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import os
import matplotlib.pyplot as plt
import requests
import json
import time
from dotenv import load_dotenv

# the path after cloning the github repo
dataset_path = '';

# the full path of the dataset for getting twitter ids 
dataset_bangla_path = os.path.join(dataset_path,
                                 'Annotated_Roman_Bengali_TweetIds.csv')

# the name of the file that you want to save after getting the tweets
name = ''

# create a .env file
# create a twitter developer account, request for elevated access
# after creating it sucessfully and getting access you'll recive 
# bearer token, api token and api secret 

# then go to your .env file and save the bearer token like 
# BEARER_TOKEN = <your bearer token>

load_dotenv() # this will load all the enviroment variables for .env file
print(dataset_bangla_path) # prints the path

df = pd.read_csv(dataset_bangla_path) # to read the csv file, for obtaining twitter ids 


token = os.environ.get('BEARER_TOKEN') # this will load the token value from the .env file



def getDataChunks(dataFrame,window,offset):
    
    """
    This method is to split the twitter ids obtained from the csv file to a list of 100 ids
    The list of 100 ids will be used to create request by converting them to string
    """
    
    count = 0 # this will check whether we have reached the end of the tweeter ids list
    ids_frame = dataFrame['Tweet_Id'] # only give the column with Tweet_Id from the csv file
    dataFrame_length = ids_frame.shape[0] # get the size of the tweet ids list
    
    while count < (dataFrame_length+window): # checking whether the count has reached the dataset length + 100 
        
        # this will give a iterator that will iterate through the dataset and for each iteration
        # will return the current position of the counter and the sliced list containing 100 tweeter ids
        
        yield count, ids_frame[count:window+count].tolist() 
        count += offset # incrementing the count with the offset

def getIDList():
    """
    This will give a list of containing list of 100 ids

    Returns
    -------
    list
        list of containing lists of 100 ids.

    """
    
    ids_List = []  
    for offset,id_list in getDataChunks(df, 100, 100):
        ids_List.append(id_list)
        
    return ids_List[:-1] # the last element is 0 since we are count upto dataset length + 100


ids_List = getIDList();




def create_request_url(ids):
    
    """
    will return url for requesting tweet data
    """
    
    tweet_ids = ids
    
    url = 'https://api.twitter.com/2/tweets?ids={}'.format(tweet_ids) # tweets ids will be the string of 100 tweet ids
    
    return url


def bearer_oath(request):
    
    """
    This will return authorization headers 
    """
    
    request.headers['Authorization'] = f"Bearer {token}" # the token will send with Bearer flag
    request.headers['User-Agent'] = "v2TweetLookupPython" # set the user agent to use python
    
    return request

def request_endpoint(url):
    
    
    
    response = requests.request('GET',url,auth=bearer_oath)  # using the request module to request to the api
    print(response.status_code)
    
    if response.status_code != 200: # checking whether the request was success
        raise Exception(f"Request retrurned an error {response.status_code} ,{response.text}")
        
    return response.json()  # return the response in json format

output_data_obj = {'Tweet':[],'Label':[]}


def create_dataset(data):
    
    """
    this method is for creating an dictionary with the tweets and its corresponding labels
    
    this dictionary will be converted to csv afterwards
    """
    
    # the response gives a list of dictonaries 
    for ob in data:
        
        
        
        tweet_id = ob['id'] # this will give the tweet id from the response
        tweet = ob['text'] # this will give the tweet data from the response
        label = df.loc[df['Tweet_Id'] == int(tweet_id),'Label'].values[0]   # this is for searching the rows with the tweeter id 
        
        
        
        output_data_obj['Tweet'].append(tweet) # appending the tweets on the dictionary
        output_data_obj['Label'].append(label) # appending the labels on the dictionary 
        
        

def save_csv():
    
    """
    this will convert the list to pandas dataframe and then from the dataframe conver it to csv
    """
    pd.DataFrame(output_data_obj).to_csv(os.path.join(dataset_path,name))  
    print(f"{name} saved sucessfully")
        


def main():
    
    """
    this will go through each of the ids in the csv file make a request on the api
    the data from the api will be then used to create the csv file
    
    300 request can be done per 15 minutes
    
    thus in 1 minute 20 request are done 
    
    after every 20 request a sleep is intialized to pause for 1 minute
    """
    ids_count = 0
    
    for i in range(0,len(ids_List)):
        request_ids = ids_List[i] # iterate through the ids 
        
        string_ids = ','.join(map(str,request_ids)) # converting the 100 ids array to a string for request
        
        url = create_request_url(string_ids) # getting the url with string id as parameter
        
        response = request_endpoint(url) # requesting at the endpoint
        
        data = response['data'] # getting the list of dictonaries from the response 
        
        create_dataset(data) # create the dictonary of the dataset from the obtained tweets
        
        ids_count += 1
        
        # sleep for 1 minute to give the server a break
        # not to slow down the sever with every request
        if ids_count == 20:
            
            print('Sleeping for 60 seconds')
            time.sleep(60)
            print('Done sleeping')
            
            ids_count = 0
            
    save_csv() # finally save the csv from the dictonary 
    
if __name__ == '__main__':
    main()
    #print(os.getcwd())
    #pass




