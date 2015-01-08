import twitter_KeywordAnalysis
import connections

if __name__ == '__main__':
	db_conn,db_cur = connections.connect_to_mysql()
	twitter_KeywordAnalysis.main(db_conn,db_cur)
