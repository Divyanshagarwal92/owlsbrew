from instagram.client import InstagramAPI
import pymysql
import re
import time
access_token = "331929664.467ede5.e11df71b266f4ece83f3a324f1dc631a" #access_token for owl's brew
user_id = "331929664"
api = InstagramAPI(access_token=access_token)
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

def media_likes(conn, cur, media_id):
	likes = api.media_likes(media_id)
	for like in likes:
		like_user_id = like.id
		sql = "INSERT IGNORE  INTO likes ( media_id, source, user_id) VALUES("'"%s"'","'"%s"'","'"%s"'")"%(media_id,'Instagram',like_user_id)	
		cur.execute(sql)
		conn.commit()
	return len(likes)

def media_comments(conn, cur, media_id):
	comments = api.media_comments(media_id)
	for comment in comments:
		comment_id = comment.id
		comment_user_id = comment.user.id
		comment_text = re.sub(r'[^a-zA-Z0-9: ]', '', comment.text)
		comment_date = comment.created_at.date()
		comment_time = comment.created_at.time()
		comment_day = comment.created_at.weekday()
		comment_hour = comment.created_at.hour
       		sql = "INSERT IGNORE INTO comment (media_id, source, comment_id, user_id, text, date, time, day, hour ) VALUES("'"%s"'","'"%s"'","'"%s"'","'"%s"'","'"%s"'","'"%s"'","'"%s"'",%d,%d)"%(media_id,'Instagram',comment_id,comment_user_id,comment_text,comment_date,comment_time, comment_day, comment_hour)
		cur.execute(sql)
		conn.commit()
	return len(comments)

def media_tags(conn, cur, media):
	media_id = media.id
	tags = media.tags
	for tag in tags:
		sql = 'INSERT IGNORE INTO tags (media_id, tag, source) VALUES("'"%s"'","'"%s"'","'"%s"'")'%(media_id,tag.name,'Instagram')
		cur.execute(sql)
		conn.commit()

def user_media( conn, cur):
	recent_media, next_ = api.user_recent_media( user_id="331929664" )
	while next_:
		more_media, next_ = api.user_recent_media(with_next_url=next_)
		recent_media.extend(more_media)
	
	print 'Media collected. No of entries: ' + str(len(recent_media))
	for media in recent_media:
		media_id = media.id
		num_likes = 0 
		num_comments = 0 
		date = media.created_time.date()
		time = media.created_time.time()
		day = media.created_time.weekday()
		hour = media.created_time.hour
		filter_  = media.filter 
		url = media.link
		print media.id
		
		try:
			if media.location != None:
				latitude = media.location.point.latitude
				longitude = media.location.point.longitude
		except:
			print 'Location not found'
			latitude = 0 
			longitude = 0

		if media.caption != None :
			text = re.sub(r'[^a-zA-Z0-9: ]', '', media.caption.text)
		else:
			text ='None'
		
		if media.comments != None: #Media having no comments not included in this table
			num_comments = media_comments( conn, cur, media_id)
			
		if media.likes!= None: #Media having no likes not included in this table
       			num_likes = media_likes( conn, cur, media_id)
		
		# if media id already present in db table - 'tags' skip this call.
		if hasattr(media, 'tags'):
			media_tags( conn, cur, media)
			print "tags----", media.tags
		
		print 'No Likes: ' + str(num_likes)
		print 'No Comments: ' + str(num_comments)
		
		check_sql_for_media = "SELECT EXISTS(SELECT 1 FROM media WHERE media_id = '" + media_id + "')"
		cur.execute(check_sql_for_media)
		result = cur.fetchone()

		if result[0] == 0 : # INSERT if media not present 
			print 'Inserting media_id: ' + media_id
			insert_sql = "INSERT IGNORE INTO media (media_id, source, text, num_likes, num_comments, date, time, day, hour, filter, latitude, longitude, url) VALUES("'"%s"'","'"%s"'","'"%s"'",%d,%d,"'"%s"'","'"%s"'",%d,%d,"'"%s"'",%f,%f,"'"%s"'")"%(media_id,'Instagram',text,num_likes,num_comments,date,time,day,hour,filter_,latitude,longitude,url)
                	cur.execute(insert_sql)
	        	conn.commit()
		else: # update num_likes and num_comments for existing media.
			print 'Updating media_id: ' + media_id
			update_sql = "UPDATE media SET url='" + url + "', num_likes=" + str(num_likes) + ", num_comments = " + str(num_comments) + " WHERE media_id = '" + media_id + "'" 
			cur.execute(update_sql)
	        	conn.commit()


		print '\n\n'

def user_profile( conn, cur):
	print 'Updating instaprofile'
	user = api.user(331929664)
	num_media = user.counts['media']
	num_followed_by = user.counts['followed_by']
	num_followers = user.counts['follows']
	time_id = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())

	sql = "SELECT COUNT(*) FROM comment"
	cur.execute(sql);
	result=cur.fetchone()
	num_comments = result[0]
	
	sql = "SELECT SUM(num_likes) FROM media"
	cur.execute(sql);
	result1=cur.fetchone()
	num_likes = result1[0]
	
	sql = "SELECT COUNT(*)  FROM comment"
	cur.execute(sql);
	result2=cur.fetchone()
	num_media = result2[0]
	
	print "No of comments:" + str(num_comments)
	print "No of likes:" + str( num_likes)
	sql = "INSERT IGNORE INTO instaprofile (timestamp_id, media, likes, comments, followers, followed_by) VALUES("'"%s"'",%d,%d,%d,%d,%d)"%(time_id, num_media, num_likes, num_comments, num_followers, num_followed_by)
        cur.execute(sql)
	conn.commit()


db_conn,db_cur = connect_to_mysql()
user_media( db_conn, db_cur)
user_profile(db_conn, db_cur)
