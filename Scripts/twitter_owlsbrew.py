import twitter
import json
import re
import pymysql
import time
from boto.sqs.message import Message


#This script extracts tweets for a given keyword search term from "twitter search api" and inserts into the mysql database.
# Once tweet is inserted successfully, a message is written to amazon sqs queue with "start_id_str" and "end_id_str"

#create a twitter app and fetch the twitter credentials
consumer_key = 'dIoeyySs7S8u4sXsnEzslRCpw'
consume_secret = 'qrVJgL01cbzvBMOoCS8btCsQDbadyV9ZtFORPqDZGy5iPbAHoB'
access_token = '38030048-A7KIAUUE2qL1LUSruwjwDqrVp6H0KNN0GL7huaJAD'
access_token_secret = '5jxN4IlNGcjPKHIcUTPq6Cmv50UmLvYM0tqmnJTBIQEnD'

#authenticate login with credentials
auth = twitter.oauth.OAuth(access_token, access_token_secret,consumer_key,consume_secret)
twitter_api = twitter.Twitter(auth=auth)

def pretty_print_json(j):
	
	print json.dumps(j, indent=4, sort_keys=True)

def print_json(fields):
 	ls = [x[0]+": "+x[1] for x in fields];
	for item in ls: print item;

def followers(twitter_api, q):

	followers = [];
	ret = twitter_api.followers.ids(screen_name = q, cursor = -1)
	followers = followers + ret['ids'];

	next_cursor = ret['next_cursor'];	

	while next_cursor != 0:
		time.sleep(1);
		ret = twitter_api.followers.ids(screen_name = q, cursor = next_cursor)
		followers = followers + ret['ids'];	
		next_cursor = ret['next_cursor'];
		
	return followers;

def get_retweets(twitter_api, q, max_id_str):

	retweets = [];

	if max_id_str == '0':
		ret = twitter_api.statuses.retweets_of_me(q=q, count=50);	
		print ret;
	else:    
		print "Max ID : ", max_id_str;
		ret = twitter_api.statuses.retweets_of_me(q=q, count=50, since_id=max_id_str);	
	
	for item in ret:
		date = item['created_at'];
		id = item['id_str'];
		retweets.append([date, id]);

	return retweets;	

def get_mentions(twitter_api, q, max_id_str, **kw):

	mentions = [];
	if max_id_str == '0':
		ret = twitter_api.statuses.mentions_timeline(q=q, count=50);	
	else:    
		print "Max ID : ", max_id_str;
		ret = twitter_api.statuses.mentions_timeline(q=q, count=50, since_id=max_id_str);	
		print ret;

	for item in ret:
		date = item['created_at'];
		id = item['id_str'];
		mentions.append([date, id]);
	
	return mentions;	

#Extract data from Twitter using the Get search API call to twitter
def twitter_search(twitter_api, q, max_id_str,max_results=1000, **kw):

	# See https://dev.twitter.com/docs/api/1.1/get/search/tweets and 
	# https://dev.twitter.com/docs/using-search for details on advanced 
	# search criteria that may be useful for keyword arguments
	
	# See https://dev.twitter.com/docs/api/1.1/get/search/tweets    
	
	#if max_id_str == 0:
	#    search_results = twitter_api.search.tweets(q=q, count=50,request_type='recent',**kw)        
	#else:    
	#    search_results = twitter_api.search.tweets(q=q, count=50,request_type='recent',since_id=max_id_str, **kw)        
	
	search_results = twitter_api.search.tweets(q=q, count=100, **kw)        
	statuses = search_results['statuses']
	#print "length of status = %d"%len(statuses)
	
	# Iterate through batches of results by following the cursor until we
	# reach the desired number of results, keeping in mind that OAuth users
	# can "only" make 180 search queries per 15-minute interval. See
	# https://dev.twitter.com/docs/rate-limiting/1.1/limits
	# for details. A reasonable number of results is ~1000, although
	# that number of results may not exist for all queries.
	
	# Enforce a reasonable limit
	max_results = min(1000, max_results) 			#enforcing limit of 1000 tweets
	print "max_results " + str(max_results)
	for _ in range(30): 
		try:
		    next_results = search_results['search_metadata']['next_results']
		except KeyError, e: 						# No more results when next_results doesn't exist
		    print "I am here"
		    break
		    
		# Create a dictionary from next_results, which has the following form:
		# ?max_id=313519052523986943&q=NCAA&include_entities=1
		kwargs = dict([ kv.split('=') 
		                for kv in next_results[1:].split("&") ])
		
		search_results = twitter_api.search.tweets(**kwargs)
		statuses += search_results['statuses']
		
		#print len(statuses)
		if len(statuses) > max_results: 
		    break
	return statuses

def read_twitter_json(s):
#fields : (id_str,created_at, latitude, longitude, fav_count, retweet_count, 
#		   text, lang, hashtage, URLs, mentions/tags, username, user_location, time_zone, 
#	       retweeted_from_username, retweeted_from_user_location)
	fields = []; 
	fields.append(('id_str',s['id_str']));
	fields.append(('created_at',s['created_at']));
	try:
		fields.append(('latitude',str(s['coordinates']['coordinates'][0])))
	except TypeError:
		fields.append(('latitude',"null"));
	try:
		fields.append(('longitude',str(s['coordinates']['coordinates'][1])));
	except TypeError:
		fields.append(('longitude',"null"));
	fields.append(('fav_count', str(s['favorite_count'])));
	fields.append(('retweet_count', str(s['retweet_count'])));
	fields.append(('text', re.sub(r'[^a-zA-Z0-9: ]', '', s['text'].encode('ascii','ignore'))));
	fields.append(('lang', s['lang']));
	fields.append(('hashtags', ','.join([ x['text'].encode('ascii','ignore') for x in s['entities']['hashtags']])));
	fields.append(('urls', ','.join([x['display_url'].encode('ascii','ignore') for x in s['entities']['urls']])));
	fields.append(('user_mentions', ','.join([x['screen_name'] for x in s['entities']['user_mentions']])));
	try:
		fields.append(('username', s['user']['screen_name']));
	except (UnicodeDecodeError, UnicodeEncodeError):
		fields.append(('username', "null"));
	fields.append(('user_location', s['user']['location'].encode('ascii','ignore')));
	if s['user']['time_zone'] is None:
		fields.append(('time_zone', "null"));
	else:	
		fields.append(('time_zone', s['user']['time_zone']));
	try:
		fields.append(('retweet_from_username', s['retweeted_status']['user']['screen_name']));
	except (KeyError,TypeError):
		fields.append(('retweet_from_username', "null"));
	try:
		fields.append(('retweet_from_user_location', s['retweeted_status']['user']['location'].encode('ascii','ignore')));
	except (KeyError,TypeError):
		fields.append(('retweet_from_user_location', "null"));
		
	print fields;   
	return fields;

def insert_into_mysql(conn,cur,processed_tweets,q,start_id_str,end_id_str):
	no_of_records_inserted = 0 
	search_string = q 
	for tweet_fields in processed_tweets:
		field_names = [x[0] for x in tweet_fields];
		field_values = [x[1] for x in tweet_fields];
		#SQL query
		sql = "INSERT IGNORE INTO \
		tweets_new(search_string,"+field_names[0]+","+field_names[1]+","+field_names[2]+","+field_names[3]+","+field_names[4]+","+field_names[5]+","+field_names[6]+","+field_names[7]+","+field_names[8]+","+field_names[9]+","+field_names[10]+","+field_names[11]+","+field_names[12]+","+field_names[13]+","+field_names[14]+","+field_names[15]+")"+ \
		" VALUES (\"%s\",\" %s\", \"%s\", \"%s\", \"%s\", %s, %s, \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\")" \
		%(search_string,field_values[0],field_values[1],field_values[2],field_values[3], field_values[4], field_values[5], field_values[6], field_values[7], field_values[8], field_values[9], field_values[10], field_values[11], field_values[12], field_values[13], field_values[14], field_values[15]);
		print sql;
		cur.execute(sql)
		conn.commit()
		no_of_records_inserted = no_of_records_inserted + 1 
	print "no_of_records_inserted into mysql = %d"%no_of_records_inserted    
	return no_of_records_inserted

def insert_mentions(conn, cur, mentions):

	num = 0;	
	for mention in mentions:
	
		sql = "INSERT IGNORE INTO twitter_mentions(id_str, created_at) VALUES (\"%s\", \"%s\")" % (mention[1], mention[0]);
		cur.execute(sql);
		conn.commit();
		
		num += 1;

	return num;

def insert_retweets(conn, cur, retweets):

	num = 0;
	for retweet in retweets:
	
		sql = "INSERT IGNORE INTO retweets(id_str, created_at) VALUES (\"%s\", \"%s\")" % (retweet[0], retweet[1]);
		cur.execute(sql);
		conn.commit();
		num += 1;

	return num;

def insert_into_sqs(sqs_queue,start_id_str,end_id_str,q):
#Creating Msg - It contains start_id, end_id of the tweets and the search term
    m = Message()
    msg = start_id_str + '|' + end_id_str + '|' + q
    print msg
    m.set_body( msg );
    sqs_queue.write(m)
    print "after sqs write"


#Fetch the max value of id_str from the mysql database. Twitter search api is called only for the newest data that is not present in mysql
def fetch_max_id_str(cur,q):

    q = q.split('\'');
    q = '\'\''.join(q);
    sql = "select max(id_str) from tweets where search_string = " + "'" + q + "'";
    cur.execute(sql);
    output = cur.fetchall()

    if (output[0][0] is None) or (len(output[0][0]) == 0):
        max_id_str = 0
    else:
        max_id_str = int(output[0][0])
    return max_id_str


def fetch_max_id_str_mentions(cur,q,sw):

	if sw == 0:
		sql = "select max(id_str) from twitter_mentions";
	else: 
		sql = "select max(id_str) from retweets";
	cur.execute(sql);
	output = cur.fetchall();

	if (output[0][0] is None) or (len(output[0][0]) == 0):
	    max_id_str = '0';
	else:
	    max_id_str = output[0][0];
	
	return max_id_str;

def tweets_main(q,conn,cur,sqs_queue):
	#print strftime("%Y-%m-%d %H:%M:%S", gmtime())
	
	#fetch the max id_str from the mysql database. Idea is to fetch only the latest rows since last fetch from twitter    
	#max_id_str = fetch_max_id_str(cur,q)
	#results = twitter_search(twitter_api, q, max_id_str,max_results=3000)
	
	#foll = followers(twitter_api, 'TheOwlsBrew');
	#print len(foll);

	#max_id_str = fetch_max_id_str_mentions(cur,'TheOwlsBrew',0);
	max_id_str = '0';
	mentions = get_mentions(twitter_api, 'SrBachchan', max_id_str);
	#print "Number of mentions inserted : ", insert_mentions(conn, cur, mentions);
	for mention in mentions:
		print mention[1], mention[0];
