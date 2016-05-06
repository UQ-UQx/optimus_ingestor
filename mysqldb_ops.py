import MySQLdb
import utils
import config
import sys


# Ingestor system database name
INGESTOR_DB = 'ingestor'

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
	        # if no 'db' provided, INGESTOR_DB will be the default db
	        if 'db' in kwargs:
	            mysql_conn.select_db(kwargs['db'])
	        else: 
	            mysql_conn.select_db(INGESTOR_DB)
	        
	        cur = mysql_conn.cursor()
	        if 'sql' in kwargs:
	            cur.execute(kwargs['sql'])
	        if 'sql_arr' in kwargs:
	            for sql in kwargs['sql_arr']:
	                cur.execute(sql)
	        mysql_conn.commit()
	        cur.close()	            
	except Exception, e:
	    logger.error(e)

def execute_query(**kwargs):
	global mysql_conn
		
	try:
	    try:
	        mysql_conn.ping(True)
	    except MySQLdb.OperationalError:
	        mysql_conn = connect_database()
	    finally:
	    	# Get cur
	    	if 'cur' in kwargs:
	    		cur = kwargs['cur']
	    	else:
	    		# if no 'db' provided, INGESTOR_DB will be the default db
		        if 'db' in kwargs:
		            mysql_conn.select_db(kwargs['db'])
		        else: 
		            mysql_conn.select_db(INGESTOR_DB)		        
		        cur = mysql_conn.cursor()

	        if 'sql' in kwargs:
	            cur.execute(kwargs['sql'])

	        return cur	            
	except Exception, e:
	    logger.error(e)

def close_cur(cur):
	if cur:
		cur.close()

	