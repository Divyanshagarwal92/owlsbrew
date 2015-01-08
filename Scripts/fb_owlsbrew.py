
# coding: utf-8

# In[106]:

import facebook
import sys
import urlparse
import os
import httplib
import simplejson as json
import requests
from pandas import DataFrame
import pymysql

#ACCESS_TOKEN = 'CAACEdEose0cBAJZBQUHxaztxbZCcFoMs5EDGiTBosImZAozh90O8fjXdZCcbZCtYIXniUAPJ92oeWOwvxYjZBTRswX5nl4WuHZB500DewZCJFko3LZC4dW1QcRR27Nxme6UV2nPEsueKD6lKXsZCF9S0FTlHD64CMlVK4s2nnYA5Uj2ZCjUghaxWckOtTK7Ww8VaNJ6i6KEBadtH1fjWOSv9lC8'
access_token = "CAACEdEose0cBAL7M1pZBuZB9FSZA7uzLZANQOLuNTEcfl5SKZAsjI0ZACp8UUhhTjD4mWImuT2WmEuzfriYXJO3Ev50MZB033waww8GP0E3YZAiBO2tCqKwE9cdnRY5Fa4rsNHijYuNnvxjElCxcSdr2GK0zmJJwxC2CCnmyN85cyQAz6ZA3YtWzxHAnrl6pcqJgcLw9w9lxVpNwGJdZCZBJUVJ"
owlsbrew_id = '615981785082505'
graph = facebook.GraphAPI(access_token)


# In[68]:

#Connect to mysql
def connect_to_mysql():
    host_name='twittmap.ct78jelemnjv.us-east-1.rds.amazonaws.com'
    port_num=3306
    user_name='ebroot'
    password='JoshPriya9' 
    database='owlsbrew'
    charset = 'utf8'

    conn = pymysql.connect(host=host_name,
                           port=port_num, user=user_name,
                           passwd=password, 
                           db=database,
                           charset = charset);
    cur = conn.cursor();
    return conn,cur
conn,cur = connect_to_mysql()
print conn,cur


# In[71]:

#Fetch likes for each of the post
def getmore(url):
    req = requests.get(url)
#    print url
    f = req.text
    results = json.loads(f)
    return results

def construct_url(post_id,access_token):
    url = "https://graph.facebook.com/%s/likes?access_token=%s"%(post_id,access_token)
    return url

def build_results(likes_results):
    dict_likes_results = {'id':[],'name':[]}
    
    try:
        len_likes = len(likes_results['data'])
    except:
        len_likes = 0
        
    for i in range(len_likes):
        dict_likes_results['id'].append(likes_results['data'][i]['id'])
        dict_likes_results['name'].append(likes_results['data'][i]['name'])

    more_fetch = True
    try:
        while likes_results['paging']['next'] and more_fetch:
            #To fetch more likes
            likes_url = likes_results['paging']['next']
            likes_results = getmore(likes_url)
            try:
                len_likes = len(likes_results['data'])
            except KeyError:
                len_likes = 0
                
            for i in range(len_likes):
                dict_likes_results['id'].append(likes_results['data'][i]['id'])
                dict_likes_results['name'].append(likes_results['data'][i]['name'])
    except KeyError:
        more_fetch = False        

    df_results = DataFrame.from_dict(dict_likes_results,orient='columns')
    return df_results

def main_likes(post_id,access_token):
    likes_url = construct_url(post_id,access_token)
    likes_results = getmore(likes_url)
    df_results = build_results(likes_results)
    return df_results
    
#if __name__ == "__main__":

#    post_id = "615981785082505_986168684730478"
#    post_id = "615981785082505_996631280350885"
#    access_token = "CAACEdEose0cBAPJqIzjlfOzNlzl99C0lbV3VQ8Mr30xnZBOiwPoEvhYI8xeN8afcxfJYbon70BQQZCeQ6Wa0F7s27islsHqBxZA14VXyZCLf7EhgtbSffzKkbav7MgLRw1mJnLbHCltzwQekBC7yjcmbn87wMeCHLCUxa8NoutnPXK8UeIuQn3RVI9qbMsBqm0aydy6d7qYloZCZBeD6ZA9"

#    df_likes = main_likes(post_id,access_token)
#    print df_likes
    #post_id = "615981785082505_995274767153203"


# In[ ]:


connection_type = 'feed'
total_posts = 0

dict_posts = {'post_id':[],'type':[],'created_time':[],'updated_time':[],'created_by_name':[],'created_by_id':[],             'link':[],'no_of_likes':[],'message':[],'description':[],'no_of_comments':[] }

dict_comments={'post_id':[],'comment_id':[],'comment':[],'commented_by_id':[],'commented_by_name':[],'commented_time':[],             'no_of_likes':[]}

try:
  feed = graph.get_connections(owlsbrew_id, connection_type, limit=1000)
  while 'paging' in feed and 'next' in feed['paging'] and feed['paging']['next']:
      no_of_posts = len(feed['data'])
      total_posts += len(feed['data'])
      for i in range(no_of_posts):
          dict_posts['post_id'].append(feed['data'][i]['id'])
          dict_posts['type'].append(feed['data'][i]['type'])
          dict_posts['created_time'].append(feed['data'][i]['created_time'])
          dict_posts['updated_time'].append(feed['data'][i]['updated_time'])
          dict_posts['created_by_name'].append(feed['data'][i]['from']['name'])
          dict_posts['created_by_id'].append(feed['data'][i]['from']['id'])
          
          #comments section
          try:
              data_comments = feed['data'][i]['comments']
              no_of_comments = len(data_comments['data'])
              post_id = feed['data'][i]['id']
              for m in range(no_of_comments):
                  comment_id = data_comments['data'][m]['id']
                  comment = data_comments['data'][m]['message']
                  commented_by_id = data_comments['data'][m]['from']['id']
                  commented_by_name = data_comments['data'][m]['from']['name']
                  commented_time = data_comments['data'][m]['created_time']
                  no_of_likes = data_comments['data'][m]['like_count']

                  dict_comments['post_id'].append(post_id)
                  dict_comments['comment_id'].append(comment_id)
                  dict_comments['comment'].append(comment)
                  dict_comments['commented_by_id'].append(commented_by_id)
                  dict_comments['commented_by_name'].append(commented_by_name)
                  dict_comments['commented_time'].append(commented_time)
                  dict_comments['no_of_likes'].append(no_of_likes)

#                df_comments = extract_comments(data_comments,dict_posts['post_id'])
          except KeyError:
              pass
          
          try:
              dict_posts['link'].append(feed['data'][i]['link'])
          except KeyError:
              dict_posts['link'].append('Null')

          try: 
              post_id = feed['data'][i]['id']
              df_likes = main_likes(post_id,access_token)
              total_no_of_likes = df_likes.shape[0] #number of rows of the resultant dataframe
     #         print "post_id, no of likes = %s,%d"%(post_id,total_no_of_likes)
              dict_posts['no_of_likes'].append(total_no_of_likes)    
          except KeyError:
              dict_posts['no_of_likes'].append(0)

          try:
              dict_posts['message'].append(feed['data'][i]['message'])
          except:
              dict_posts['message'].append('Null')
          
          try:
              dict_posts['description'].append(feed['data'][i]['description'])
          except:
              dict_posts['description'].append('Null')

      print 'owlsbrew_count_facebook_post @ %s total_posts'%total_posts

      nextUrl = feed['paging']['next']
      parsed = urlparse.urlparse(nextUrl)
      until = int(urlparse.parse_qs(parsed.query)['until'][0])
      feed = graph.get_connections(owlsbrew_id, connection_type, limit=1000, until=until)
  total_posts += len(feed['data'])
  print 'owlsbrew_count_facebook_posts FINISHED %s total_posts'%total_posts
except:
  print "Unexpected error:", sys.exc_info()[0]
  raise


# In[76]:

df_posts  = DataFrame.from_dict(dict_posts, orient='columns')


# In[100]:

def run_query(sql):
    try:
        cur.execute(sql)
        output = cur.fetchall()
        conn.commit()
        return output
    except pymysql.err.InternalError, e:
        print 'A MySQL error occured!\n'
        print 'If you need to consult the DB schema please use either '        'the DBCONNECT.get_field_info() or DBCONNECT'        '.get_table_names() methods.\n'
        print 'Error details:'
        return e


# In[111]:

df_comments = DataFrame.from_dict(dict_comments,orient='columns')


# In[114]:

total_no_of_comments = df_comments.shape[0]

for m in range(total_no_of_comments):
    post_id = df_comments.iloc[m]['post_id']
    comment_id = df_comments.iloc[m]['comment_id']
    comment = re.sub(r'[^a-zA-Z0-9: ]','',df_comments.iloc[m]['comment'])
    commented_by_id = df_comments.iloc[m]['commented_by_id'] 
    commented_by_name = df_comments.iloc[m]['commented_by_name'] 
    commented_time = df_comments.iloc[m]['commented_time'] 
    no_of_likes = df_comments.iloc[m]['no_of_likes'] 

    sql = 'insert into fb_comments values("'"%s"'","'"%s"'","'"%s"'","'"%s"'","'"%s"'","'"%s"'",%d)'    %(post_id,comment_id,comment,commented_by_id,commented_by_name,commented_time,no_of_likes)
    run_query(sql)


