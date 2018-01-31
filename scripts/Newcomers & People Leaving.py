
# coding: utf-8

# # Authors first and last commit date
# 
# This notebooks focus on retrieving data from ES and using Pandas to perform some basic analysis on it.
# 

# ## Import libraries
# 
# First we need to import those Python modules we are going to use. We could import them at any point before using them.

# In[1]:


import certifi
import configparser
import json
import os
import sys

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


# ## Declaring functions
# We can define new functions at any point. In this case we decided to declare 'create_conn' function here at the begining because is a generic function. In fact we could create a separate module with this kind of general functions and import that module in every notebook we need to create an ES connection.
# 
# Notice we are pointing to '../.settings' file to use same config as in plain script version of this code.

# In[2]:


def create_conn():
    """Creates an ES connection from ''.settings' file.

    ''.settings' contents sample:
    [ElasticSearch]

    user=john_smith
    password=aDifficultOne
    host=my.es.host
    port=80
    path=es_path_if_any
    """

    parser = configparser.ConfigParser()
    parser.read('../.settings')

    section = parser['ElasticSearch']
    user = section['user']
    password = section['password']
    host = section['host']
    port = section['port']
    path = section['path']

    connection = "https://" + user + ":" + password + "@" + host + ":" + port                 + "/" + path

    es_read = Elasticsearch([connection], use_ssl=True,
                            verity_certs=True, ca_cert=certifi.where(),
                            scroll='300m', timeout=1000)

    return es_read


# ## Querying ES
# Following code creates an ES connection and executes the query to get, for each author, first and last commit dates. 
# 
# In order to get unique authors, a bucket is created using 'author_uuid' field.
# 
# Notice that a filter is applied to get data for whole years. Thus, we exclude data for current year.

# In[3]:


"""Query ES to get first and last commit of each author together with
some extra info like .
"""
es_conn = create_conn()

# Create search object
s = Search(using=es_conn, index='git')

# FILTER: retrieve commits before given year
s = s.filter('range', grimoire_creation_date={'lt': 'now/y'})

# Bucketize by uuid and get first and last commit (commit date is stored in
# author_date field)
s.aggs.bucket('authors', 'terms', field='author_uuid', size=10000000)     .metric('first', 'top_hits',
            _source=['author_date', 'author_org_name', 'author_uuid', 'project'],
            size=1,
            sort=[{"author_date": {"order": "asc"}}]) \
    .metric('last_commit', 'max', field='author_date')

# Sort by commit date
s = s.sort("author_date")

result = s.execute()


# # Print results
# 
# From here, we can start playing with the data, but first we can print those results to have a look at them.
# 
# Notice we can use variables from other cells that were executed previosly (look at numbers between square brackets if not sure about execution order).

# In[4]:


from pprint import pprint

result_buckets = result.to_dict()['aggregations']['authors']['buckets']

pprint(result_buckets)


# ## Create Pandas dataframe
# In order to work with data, we create a Pandas dataframe where each row will contain:
# * Author UUID
# * First commit date
# * Last commit date
# * Author org name
# * Project

# In[5]:


import pandas as pd
from datetime import datetime

# Get a dataframe with each author and their first commit
buckets = []
for bucket_author in result_buckets:
    author = bucket_author['key']

    first = bucket_author['first']['hits']['hits'][0]
    first_commit = first['sort'][0]/1000
    last_commit = bucket_author['last_commit']['value']/1000
    org_name = first['_source']['author_org_name']
    project = first['_source']['project']
    
    buckets.append({
            'first_commit': datetime.utcfromtimestamp(first_commit),
            'last_commit': datetime.utcfromtimestamp(last_commit),
            'author': author,
            'org': org_name,
            'project': project
    })
    
authors_df = pd.DataFrame.from_records(buckets)
authors_df.sort_values(by='first_commit', ascending=False,
                        inplace=True)

pprint(authors_df)


# ## Newcomers per year
# Next we will use pandas to group data and count the number of newcomers per year and organization.

# In[6]:


# Group by year of first commit and project, counting number of authors
first_df = authors_df.groupby([authors_df.first_commit.dt.year, authors_df.org])                        .agg({'author': pd.Series.nunique})
first_df = first_df.reset_index()
first_df.rename(columns={"first_commit": "year", "author": "newcomers"}, inplace=True)
first_df = first_df.sort_values(by=['year', 'newcomers'], ascending=[False, False])


# In[7]:


# Get top 20 projects based on newcomers from 2008
newcomers_df = pd.DataFrame()
for year in first_df['year'].unique():
    if year > 2008:
        year_df = first_df.loc[first_df['year'] == year].head(20)
        newcomers_df = pd.concat([newcomers_df, year_df])
        
pprint(newcomers_df)


# ## People Leaving
# 

# In[8]:


# Group by year of last commit and project, counting number of authors
last_df = authors_df.groupby([authors_df.last_commit.dt.year, authors_df.org])                        .agg({'author': pd.Series.nunique})
last_df = last_df.reset_index()
last_df.rename(columns={"last_commit": "year", "author": "leaving"}, inplace=True)
last_df = last_df.sort_values(by=['year', 'leaving'], ascending=[False, False])


# In[9]:


# Get top 20 projects based on newcomers from 2008
leaving_df = pd.DataFrame()
for year in last_df['year'].unique():
    if year > 2008:
        year_df = last_df.loc[last_df['year'] == year].head(20)
        leaving_df = pd.concat([leaving_df, year_df])
        
pprint(leaving_df)


# ## Merge both dataframes
# 
# Now we put everything together to keep track of people joining and leaving the community through years.

# In[10]:


final_df = newcomers_df.merge(leaving_df, on=['year','org'], how='outer')
final_df = final_df.fillna(0)
final_df = final_df.sort_values(by=['year', 'org'], ascending=[False, False])

pprint(final_df)


# ## Plot a chart on Newcomers & People Leaving
# 
# Finally, to visualize data we can use any library we like. In this example we will use [Plot.ly](https://plot.ly/python/), an easy to use Python library with a number of differents graphs available.
# 
# We will build a chart with several lines:
# * Evolution of newcomers in each organization.
# * Evolution of people leaving in each organization.
# * Evolution of the difference (newcomers - leaving) in each organization.
# 
# **Notice chart is customizable, so you can select only those lines you want to view.**
# 
# 
# 

# In[13]:


import plotly as plotly
import plotly.graph_objs as go

plotly.offline.init_notebook_mode(connected=True)

years = final_df.year.unique()
orgs = final_df.org.unique()

data = []
for org in orgs:
    newcomers = []
    leaving = []
    both = []
    for year in years:
        if year in final_df[final_df['org'] == org]['year'].unique():
            n = final_df[(final_df['org'] == org) & (final_df['year'] == year)]['newcomers'].values[0]
            l = final_df[(final_df['org'] == org) & (final_df['year'] == year)]['leaving'].values[0]
            s = n - l
        else:
            n = 0
            l = 0
            s = 0
        newcomers.append(n)
        leaving.append(l)
        both.append(s)
        
    data.append(
        go.Scatter(
            x = years,
            y = newcomers,
            mode = 'lines+markers',
            name = org + ' newcomers'
        )
    )
    data.append(
        go.Scatter(
            x = years,
            y = leaving,
            mode = 'lines+markers',
            name = org + ' leaving'
        )
    )
    data.append(
        go.Scatter(
            x = years,
            y = both,
            mode = 'lines+markers',
            name = org
        )
    )  
    
        

plotly.offline.iplot(data, filename='evolution_chart.html')    

