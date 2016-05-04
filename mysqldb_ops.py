import MySQLdb
import utils
import config
import sys

logger = utils.getLogger(__name__)

def connect_database():
	try:
	    logger.debug('Testing MySQL Connection')
	    conn = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD)
	    logger.debug('MySQL Connection is built')
	except MySQLdb.Error, e:
	    logger.critical('Cannot build MySQL connection.')
	    logger.error('[ERROR] %d: %s' % (e.args[0], e.args[1]))
	    logger.critical('System is exiting')
	    sys.exit(1)
	else:
	    return conn

mysql_conn = connect_database()

def execute_sql(**kwargs):
	global mysql_conn
		
	try:
	    try:
	        mysql_conn.ping(True)
	    except MySQLdb.OperationalError:
	        mysql_conn = connect_database()
	    finally:
	        # Set the default database
	        if 'db' in kwargs:
	            mysql_conn.select_db(kwargs['db'])
	        else: 
	            mysql_conn.select_db('mysql')
	        
	        cur = mysql_conn.cursor()
	        if 'query' in kwargs:
	            cur.execute(kwargs['query'])
	        if 'query_arr' in kwargs:
	            for query in kwargs['query_arr']:
	                cur.execute(query)
	        mysql_conn.commit()
	        cur.close()
	            
	except Exception, e:
	    logger.error(e)
	