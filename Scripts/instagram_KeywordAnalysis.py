'''
Getting started with Alchemy API: http://www.alchemyapi.com/developers/getting-started-guide/using-alchemyapi-with-python/
Copy alchemyapi.py alchemyapi.pyc api_key.txt these files from source folder to working directory.
'''
import operator
from collections import Counter
from nltk.corpus import stopwords
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

def get_comments_from_db(conn,cur):
	#Using a range of tweetIDs, fetches tweets from MySQL database
	comments = [];

	sql_query = "SELECT media_id, comment_id, text FROM  comment WHERE  polarity = 'positive' " ;
	print sql_query;
	cur.execute(sql_query);
	out = cur.fetchall();

	for item in out:
		if item[0] == '':
			continue;
		templs = [item[0], item[1], item[2]]
		comments.append(templs)
	return comments;

def get_posts_from_db(conn,cur):
	#Using a range of tweetIDs, fetches tweets from MySQL database
	posts = [];

	sql_query = "SELECT media_id, text FROM  media WHERE num_likes > 20"
	#print sql_query;
	cur.execute(sql_query);
	out = cur.fetchall();

	for item in out:
		if item[0] == '':
			continue;
		templs = [item[0], item[1]]
		posts.append(templs)
	return posts;

def get_mentions_from_db(conn,cur):
	#Using a range of tweetIDs, fetches tweets from MySQL database
	mentions = [];

	sql_query = "SELECT id, description FROM  mention"
	#print sql_query;
	cur.execute(sql_query);
	out = cur.fetchall();

	for item in out:
		if item[0] == '':
			continue;
		templs = [item[0], item[1]]
		mentions.append(templs)
	return mentions

def commentProcessing(comments):

	f = open('commentCloud', 'w')
	sentence = ''
	for comment in comments:
		sentence = sentence + ' . ' + comment[2]
	
	# word frequency
	newsentence = []
	stop = set(stopwords.words('english'))
	stop.add('theowlsbrew')
	for word in sentence.split():
		word = word.lower()
		if word not in stop:
			newsentence.append(word)
	
	counts = Counter(newsentence)
	sorted_counts = sorted(counts.items(), key=operator.itemgetter(1))
	print sorted_counts
	for tup in sorted_counts:
		if len(tup[0])>3 and tup[1]>=5:
			f.write(tup[0] + ' ' + str(tup[1]))
			f.write('\n')
			print tup[0] + ' ' +  str(tup[1])
	f.close()
	print '\n\n\n\n'	
	# cocktail keywords

	f = open('keywordsCocktail','w')
	response = alchemyapi.keywords('text', sentence, {'keywordExtractMode':'strict', 'maxRetrieve': 100, 'knowledgeGraph': 1} )
	graph = []
	for keyword in response['keywords']:
		phrase = keyword['text']
		'''
		try:
			graph.append(keyword['knowledgeGraph']['typeHierarchy'])
		except:
			pass
		'''
		if phrase.find('cocktail') != -1:
			f.write(phrase)
			f.write('\n')
			print phrase


def keywordExtractionPosts(posts):
	sentence = ''
	for post in posts:
		sentence = sentence + '. ' + post[1]
	stop = set(['owl', 'owls', "owl's", 'theowlsbrew', 'theowlsbrew:', 'owlsbrew','owlsbrews:'] )
	newsentence = []
	for word in sentence.split():
		newword = word.lower()
		if newword not in stop:
			newsentence.append(word)
	newsentence = ' '.join(newsentence)
	
	response = alchemyapi.keywords('text', newsentence, {'keywordExtractMode':'strict', 'maxRetrieve': 100, 'knowledgeGraph': 1} )
	keywords= []
	graph = []
	for keyword in response['keywords']:
		phrase = keyword['text']
		keywords.append(phrase)
		#print phrase, str(keyword['relevance'])
		try:
			#print keyword['knowledgeGraph']['typeHierarchy']
			graph.append(keyword['knowledgeGraph']['typeHierarchy'])
		except:
			pass
	graph.sort()
	for val in graph:
		print val
	return keywords

db_conn, db_cur = connect_to_mysql()

comments = get_comments_from_db(db_conn, db_cur)
for comment in comments:
	print comment[2]

#commentProcessing(comments)

#posts = get_posts_from_db(db_conn, db_cur)
#keywords = keywordExtractionPosts(posts)
'''
def postProcessing(keywords):
	line = ' '.join(keywords)
	words = line.split()
	counts = Counter(words)
	stopwords = ['cocktails', 'cocktail', 'tea', 'brew', 'owlsbrew', 'theowlsbrew', 'owls', "owl's", 'owl', 'RT', 'theowlsbrew:', 'cocktail:']
	for key in counts.keys():
		flag = 0
		for word in stopwords:
			if key == word:
				flag = 1
				break
		if counts[key] >= 2 and flag == 0:
			print key, counts[key]

'''
