'''
Getting started with Alchemy API: http://www.alchemyapi.com/developers/getting-started-guide/using-alchemyapi-with-python/
Copy alchemyapi.py alchemyapi.pyc api_key.txt these files from source folder to working directory.
'''
from alchemyapi import AlchemyAPI
alchemyapi = AlchemyAPI()
import pymysql
def connect_to_mysql():
	#database credentials
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

def get_posts_from_db(conn,cur):
	#Using a range of tweetIDs, fetches tweets from MySQL database
	posts = [];

	sql_query = "SELECT media_id, comment_id, text FROM  comment WHERE  polarity = 'positive' " ;
	print sql_query;
	cur.execute(sql_query);
	out = cur.fetchall();

	for item in out:
		if item[0] == '':
			continue;
		templs = [item[0], item[1], item[2]]
		print templs
		posts.append(templs)
	#       print tweets
	#Return tweet IDs and text in a list
	return posts;

def sentiment_analysis(posts):
	#posts is a list of tuples of the form (ID, text)
	#Outputs a list of tuples of the form (ID, text, polarity, polarity_score)

	sent_posts = []
	count = 0;
	for post in posts:
		if count == 5: 
			break
		count = count + 1 
		response = alchemyapi.sentiment("text", post[2])
		if response['status'] == 'ERROR':
			sent_posts.append([ 'unknown', 0.0 ])
			continue;
		type = response["docSentiment"]["type"]

		if type.encode('ascii') == 'neutral':
			sent_posts.append([ type, 0.0 ])
		else:
			sent_posts.append([ type, response["docSentiment"]["score"] ])
			print [ type, response["docSentiment"]["score"] ]
	#	response = alchemyapi.keywords('text', post[2] )
	#	print response
	return sent_posts

def update_posts_in_db(posts, analyzed_posts,conn,cur):
	#Updates posts in MySQL database with the sentiment analysis output

	#SQL query to update post with the sentiment analysis output 
	for i in range(0,len(analyzed_posts)):
		polarity = analyzed_posts[i][0].encode('ascii');
		polarity_score = analyzed_posts[i][1];
		update_query = "UPDATE comment SET polarity=\'" + polarity + "\', polarity_score=" + str(polarity_score) + " WHERE media_id = \'" + posts[i][0] + "\' AND comment_id= \'" + posts[i][1] + "\'";
		print update_query;
		cur.execute(update_query);
		conn.commit();

def keywordExtractionComments(posts):
	sentence = ''
	for post in posts:
		sentence = sentence + '. ' + post[2]
	print sentence
	response = alchemyapi.keywords('text', sentence, {'keywordExtractMode':'strict', 'maxRetrieve': 100} )
	print('## Keywords ##')
	for keyword in response['keywords']:
		print('text: ', keyword['text'].encode('utf-8'))
		print('relevance: ', keyword['relevance'])
		print('')
			
db_conn, db_cur = connect_to_mysql()
posts = get_posts_from_db(db_conn, db_cur)
#analyzed_posts = sentiment_analysis(posts)
#update_posts_in_db(posts, analyzed_posts, db_conn, db_cur)
keywordExtractionComments(posts)
