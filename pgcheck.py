#!/misc/PythonVirtualEnv/bin/python
####!/u01/dbaets/venv/bin/python
#####!/usr/bin/python
#
import psycopg2
import sys
import os
import argparse
import time
import getpass
from config import config
from datetime import datetime


def run_db_query(cur,sql):
    if not args.no_sqltext :

	    print "****************************  SQL TEXT ********************************\n"
	    print sql 
	    print "\n"
	    print "***********************************************************************\n"
	    print "\n"

    cur.execute(sql)
    rlist = cur.fetchall()
    return rlist


def db_size(cur, dbname):

    sql = "SELECT pg_size_pretty( pg_database_size('" + dbname + "') )"

    cur.execute(sql)
    rlist = cur.fetchall()
    return rlist

def db_conn() :
        # get a cursor
        conn = None
        try:
                # read database configuration
                params = config(filename=args.DBConfigFile)

                if 'password' not in params.keys():
                   print "Host      :" + params['host']
                   print "Port      :" + params['port']
                   print "Database  :" + params['database']
                   print "User      :" + params['user']
                   print "----------------------------------------------------------"                    
                   params['password']=getpass.getpass(prompt='Enter your password here:') 

                # connect to the PostgreSQL database
                conn = psycopg2.connect(**params)
                # create a new cursor
                cur = conn.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
                 print(error)
        return  conn, cur

# --------------------------------------
# ---- Main Program --------------------
# --------------------------------------
if (__name__ == '__main__'):
        parser = argparse.ArgumentParser(
              formatter_class=argparse.RawDescriptionHelpFormatter,
              description='''\
             PGCHECK - Report Various Info Of A PostgreSQL Database for DBA 
                         - Version 1.1 by Yu (Denis) Sun
                                 created: 6-Mar-2019
                                 updated: 21-Jun-2019
               ''')     
        parser.add_argument("DBConfigFile", nargs='?', default='database.ini', help="Specify a PostgreSQL instance connection configuration file. Default: database.ini")
        parser.add_argument("-a", "--active_session", help="Display active session ", action="store_true")
        parser.add_argument("-ad", "--age_db", help="Which database is aging ", action="store_true")
        parser.add_argument("-at", "--age_table", help="Which tables are aging.", action="store_true")
        parser.add_argument("-bl", "--blockers", help="Display blockers", action="store_true")
        parser.add_argument("-bs", "--bloat_size", help="Display top 20 bloat tables by size ", action="store_true")
        parser.add_argument("-br", "--bloat_ratio", help="Display top 20 bloat tables by ratio ", action="store_true")
        parser.add_argument("-c", "--conn_info", help="Display current connecting session related info", action="store_true")
        parser.add_argument("-cso", "--count_schema_owner", help="Table Counts By Schema And Owner", action="store_true")
        parser.add_argument("-cus", "--count_user_state", help="Session Count by Username and State", action="store_true")
        parser.add_argument("--delta", help="n seconds", default=10, type=int)
        parser.add_argument("-dbs", "--db_stats", help="Display Statistics for All Databases ", action="store_true")
        parser.add_argument("-dbsps", "--db_stats_per_sec", help="Display Per-Sec Statistics Since Last Reset for All Databases,.e.g Trascation/s ", action="store_true")
        parser.add_argument("--dbname", help="Specify a database name,used with -ds option  ")
        parser.add_argument("-ds", "--db_size", help="Display db size, --dbname option required ", action="store_true")
        parser.add_argument("-df", "--display_functions", help="List Of Functions as in psql \df", action="store_true")
        parser.add_argument("-dg", "--display_roles", help="list of rolos as in psql \dg+ ", action="store_true")
        parser.add_argument("-dt", "--desc_tab", help="Describe Table used with --schname --tabname options ", action="store_true")
        parser.add_argument("--funcname", help="Specify a function name,used with -gf option  ")
        parser.add_argument("-gf", "--get_function", help="Generate Function Definition Code, with --schname --funcname option ", action="store_true")
        parser.add_argument("-ht", "--hot_tables", help="Display top 20 hot tables for DML activities ", action="store_true")
        parser.add_argument("--limit",type=int,  help="Specify an INT value as limit to append to a query" , default=20 )
        parser.add_argument("-l", "--list_db", help="List of databases as \l+ in psql ", action="store_true")
        parser.add_argument("-niit", "--no_idle_in_transaction", help="exclude sessions with IDLE IN TRANCTION in some queries ", action="store_true")
        parser.add_argument("-nsql", "--no_sqltext", help="Don't print out sql text", action="store_true")
        parser.add_argument("-rq", "--running_query", help="Display top 10 current running queries order by duration (excluding rdsadmin)", action="store_true")
        parser.add_argument("-ps", "--pg_settings", help="Display PG settings ", action="store_true")
        parser.add_argument("--schname", help="Specify a scheman name, default public ",default='public')
        parser.add_argument("-st", "--stat_table", help="Display info from pg_stat_user_tables,  with --schname --tabname ", action="store_true")
        parser.add_argument("-std", "--stat_table_delta", help="Display delta changes and change rates from pg_stat_user_tables,  with --schname --tabname and --delta ", action="store_true")
        parser.add_argument("-sdd", "--stat_database_delta", help="Display delta  change rates from pg_stat_database  with --delta ", action="store_true")
        parser.add_argument("-sl", "--session_list", help="List sessions by different criteria default all   ", action="store_true")
        parser.add_argument("--querytext", help="query text - used with -sl")
        parser.add_argument("--event", help="event - used with -sl")
        parser.add_argument("--user", help="user - used with -sl")
        parser.add_argument("-to", "--top_objects", help="Top 20 Objects by Size ", action="store_true")
        parser.add_argument("-ts", "--top_sql", help="Display top 20 SQL from pg_stat_statement] ", action="store_true")
        parser.add_argument("--tsorder", help="Used with -ts option: tot - order by total execution time (default); avg - order by average execution time  ")
        parser.add_argument("-ti", "--table_index", help="List Indexes of A Table, require --schname --tabname options ", action="store_true")
        parser.add_argument("-tcs", "--table_colstats", help="Display Table Colume Statistics from pg_stats, require --schname --tabname options ", action="store_true")
        parser.add_argument("-tnpu", "--table_no_pkuk", help="Display All Tables Without Primary Or Unique Keys", action="store_true")
        parser.add_argument("--tabname", help="Specify a tablename name", default='t')
        parser.add_argument("-v", "--version", help="Display PG version ", action="store_true")
        parser.add_argument("-we", "--wait_event", help="Display Session wait event count ", action="store_true")


        if len(sys.argv) < 2:
           parser.print_usage()
           sys.exit(1)


        args = parser.parse_args()
        
        # get a cursor

        conn,cur=db_conn()

        if args.age_db :
           sql = """ SELECT datname, age(datfrozenxid) FROM pg_database ORDER BY 2 DESC LIMIT 20"""

           rlist =run_db_query(cur,sql )
           print "==================== Which database is aging? ==================="
           print "{:20} {:26} ".format( 
                  "db", "age(datforeznxid)")
           print "{:20} {:26} ".format( 
                  "--------------------", "-------------------------")
           for i in rlist:
                print "{:20} {:,} ".format(i[0],i[1]) 

        if args.age_table :
           sql = """ 
                  SELECT c.oid::regclass as table_name,
			greatest(age(c.relfrozenxid),age(t.relfrozenxid)) as age,
			pg_size_pretty(pg_table_size(c.oid)) as table_size
			FROM pg_class c
			LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
			WHERE c.relkind = 'r'
			ORDER BY 2 DESC LIMIT 20 """

           rlist =run_db_query(cur,sql )
           print "==================== Which tables are aging? ==================="
           print "{:35} {:15}  {:20}".format( 
                  "table_name", "age", "table_size")
           print "{:35} {:15}  {:20}".format( 
                  "-----------------------------------", "---------------", "--------------------")
           for i in rlist:
                print "{:35} {:,}          {:20} ".format(i[0],i[1], i[2]) 
        

        if args.session_list :

           sql = """
			SELECT datname, usename, pid, state, wait_event, current_timestamp - coalesce(xact_start, query_start) AS xact_runtime, query, application_name
			FROM pg_stat_activity
			WHERE 1=1 """

           if args.querytext :
               sql = sql + " and upper(query) like '%" + args.querytext.upper() + "%'"

           if args.event :
               sql = sql + " and upper(wait_event) like '%" + args.event.upper() + "%'"

           if args.user :
               sql = sql + " and upper(usename) like '%" + args.user.upper() + "%'"



           sql = sql + """  and pid !=pg_backend_pid() ORDER BY xact_start; """

           rlist =run_db_query(cur,sql )

           print "======================================================= "
           print "~~~~~~~~~~~~~~ List of Sessions ~~~~~~~~~~~~~~~~~~~~~~~"
           print "======================================================= "
           if not rlist :
                print "Not found any sessions!"
           else :
		  for i in rlist:
                         print "{:10} {:26} {:10} {:20}".format( 
			       "DB      :" , str(i[0])  , " USER      : ", str(i[1]) 
                         )
                         print "{:10} {:26} {:10} {:20}".format( 
			       "STATE   :" , str(i[3])  , " WAIT EVENT: ", str(i[4]) 
                         )
                         print "{:10} {:26} {:10} {:20}".format( 
			       "PID     :" , str(i[2])  , " APP NAME  : ", str(i[7]) 
                         )
                         print "{:10} {:26}".format( 
			       'RUN TIME: ' , str(i[5])
                         )
			 print 'QUERY   : \n' + str(i[6])
			 print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

                  print "Total session found: " + str(len(rlist))


        if args.desc_tab :

           sql = """
		SELECT a.attname,
		  pg_catalog.format_type(a.atttypid, a.atttypmod) as format_type,
		  case a.attnotnull when 't' then 'not null' else '' end as "NULLABLE" ,
		  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
		   FROM pg_catalog.pg_attrdef d
		   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)  as "Default"
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid in (select  c.oid
			FROM pg_catalog.pg_class c
			     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname ~ '^(""" + args.tabname + """)$'
			  AND n.nspname ~ '^(""" + args.schname +""")$'
		)
		AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum """

           rlist =run_db_query(cur,sql )
           print "==================== Column Types of Table " + args.schname + "." + args.tabname +"==================="
           print "{:20} {:26} {:10} {:38}".format( 
                  "attname", "format_type","NULLABLE","Default")
           print "{:20} {:26} {:10} {:38}".format( 
                  "--------------------", "-------------------------","-----------", "-------------------------------------")
           for i in rlist:
                print "{:20} {:26} {:10} {:38}".format(i[0],i[1],i[2],i[3]) 

        if args.get_function :
           if not args.funcname :
              print "specify a function  with --funcname --schname option" 
              sys.exit(1)
           sql ="""
                SELECT pg_get_functiondef(f.oid)
			FROM pg_catalog.pg_proc f
			INNER JOIN pg_catalog.pg_namespace n ON (f.pronamespace = n.oid)
			WHERE n.nspname = '""" + args.schname + "' and f.proname='"+args.funcname + "'"
           rlist =run_db_query(cur,sql )
           print "===========================  Function Definiton =================================="
           for i in rlist:
                print i[0]
                 
        if args.display_functions :
            sql = """
		SELECT n.nspname as "Schema",
		  p.proname as "Name",
		  pg_catalog.pg_get_function_result(p.oid) as "Result data type",
		  substr(pg_catalog.pg_get_function_arguments(p.oid),1,92) as "Argument data types",
		 CASE
		  WHEN p.proisagg THEN 'agg'
		  WHEN p.proiswindow THEN 'window'
		  WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
		  ELSE 'normal'
		 END as "Type"
		FROM pg_catalog.pg_proc p
		     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname ~ '^(""" + args.schname + ")$' ORDER BY 1, 2, 4"

            rlist =run_db_query(cur,sql )
            print "===========================  List of Functions =================================="
            print "{:11} {:42} {:21} {:93} {:9}".format(
                 "schema", "name","Result data type", "Argument data types", "Type")
            print "{:11} {:42} {:21} {:93} {:9}".format(
                 "----------", "-----------------------------------------", "--------------------", 
                 "-------------------------------------------------------------------------------------------+", "---------")
            for i in rlist:
                print "{:11} {:42} {:21} {:93} {:9}".format(i[0], i[1], i[2], i[3], i[4])

        if args.count_schema_owner:
            sql = """ select schemaname, tableowner, count(*) from pg_tables group by 1,2 order by 1,2 """

            rlist =run_db_query(cur,sql )
            print "===========================  Table Counts By Schema And Owner =================================="
            print "{:25} {:16} {:8}".format(
                 "schemaname", "tableowner","count")
            print "{:25} {:16} {:8}".format(
                   "-------------------------", "------------", "--------")
            for i in rlist:
                print "{:25} {:16} {:8}".format(i[0],i[1],i[2])
        if args.count_user_state:
            sql = """ select usename, state, count(*) from pg_stat_activity group by 1,2 order by 1,2 """

            rlist =run_db_query(cur,sql )
            print "===========================  Session Count By Usename And State =================================="
            print "{:20} {:16} {:8}".format(
                 "username", "state","count")
            print "{:20} {:16} {:8}".format(
                   "---------------", "------------", "--------")
            for i in rlist:
                print "{:20} {:16} {:8}".format(i[0],i[1],i[2])

        if args.table_no_pkuk :
            sql = """
                SELECT table_catalog, table_schema, table_name
		FROM information_schema.tables
		WHERE (table_catalog, table_schema, table_name) NOT IN
		    (SELECT table_catalog, table_schema, table_name
		       FROM information_schema.table_constraints
		      WHERE constraint_type  IN ('PRIMARY KEY', 'UNIQUE')
		    )
		 AND table_schema NOT IN ('information_schema', 'pg_catalog')
            """

            if args.dbname :
               sql += " AND table_catalog='" + args.dbname +"'"

            if args.schname :
               sql += " AND table_schema='" + args.schname +"'"

            sql += " order by 1,2,3 "

            rlist =run_db_query(cur,sql )
   	    print "======================  Display All Tables Without Primary Or Unique Keys  ======================"
   	    print "== The connecting user should either be an owner of the tables or having some privs other than ==" 
            print "== select on tables, if not the results may be not reliable, pleae make an educated decision   =="      
            print "================================================================================================="
            print "{:16} {:16} {:16}".format(
                 "database", "table_schema","table_name")
            print "{:16} {:16} {:16}".format("---------------","----------------", "----------------------") 
            for i in rlist:
                print "{:16} {:16} {:16}".format(i[0],i[1],i[2])
       

        if args.stat_database_delta :
            sql = """select 
                         case when length(datname) > 18  then
                             concat('..' , right(datname,16))
                         else
                            datname
                         end db
			,numbackends    
			,xact_commit    
			,xact_rollback  
			,blks_read      
			,blks_hit       
			,tup_returned   
			,tup_fetched    
			,tup_inserted   
			,tup_updated    
			,tup_deleted    
			,temp_files     
			,round(temp_bytes/1024/1024,0)     
              	        -- ,blk_read_time  
        	        -- ,blk_write_time 
                        ,to_char(now()::timestamp(0), 'YYYY-MM-DD HH24:MI:SS') stime
			,deadlocks      
			--,conflicts      
                from pg_stat_database """

            if args.dbname :
               sql += "where datname = '" +args.dbname + "' order by datname"
            else :
               sql += " order by datname"


            conn2, cur2=db_conn()
            rlist1 =run_db_query(cur,sql )

            print "waiting for " + str(args.delta) + " seconds ..."
            time.sleep(args.delta) 
 
            # using the second cursor to run the same query agin 
            rlist2 =run_db_query(cur2,sql )
            # print rlist1
            # print rlist2
	    print "============== Database Metrics Change Rate as Seen in PG_STAT_DATABASE  ==============="
	    print "==============       beginning sample time: " + rlist1[0][13] +"          ==============="
	    print "==============             sample duration: " + str(args.delta) + " seconds                   ==============="
            print ""
            print "{:>18} {:>8} {:>4} {:>8} {:>10} {:>10} {:>10} {:>5} {:>5} {:>5} {:>10} {:>12} {:>9}".format(
                "datname", "#bcknds","TPS", "blk_", "blk_"
                ,"tup_"
                ,"tup_"
                ,"tup_"
                ,"tup_"
                ,"tup_"
                ,"tmpfiles"
                ,"tmpbyts_M"
                ,"deadlck"
               )
            # secondline header
            print "{:>18} {:>8} {:>4} {:>8} {:>10} {:>10} {:>10} {:>5} {:>5} {:>5} {:>10} {:>12} {:>9} ".format(
                "   ", "(delta)","", "read/s", "hit/s"
                ,"rtrnd/s"
                ,"ftchd/s"
                ,"ins/s"
                ,"upd/s"
                ,"del/s"
                ,"(delta)"
                ,"(delta)"
                ,"(delta)"
               )
            print "{:>18} {:>8} {:>4} {:>8} {:>10} {:>10} {:>10} {:>5} {:>5} {:>5} {:>10} {:>12} {:>9}".format(
                  "------------------",
                  "--------",
                  "----",
                  "--------",
                  "----------",
                  "----------",
                  "----------",
                  "-----",
                  "-----",
                  "-----",
                  "----------",
                  "------------",
                  "---------"
            )
	    for (i,j) in zip(rlist1, rlist2) :
               print "{:>18} {:>8} {:4.0f} {:8.0f} {:10.0f} {:10.0f} {:10.0f} {:5.0f} {:5.0f} {:5.0f} {:>10} {:>12} {:>9}".format(
                    i[0]
                  , str(i[1])+"("+str(j[1]-i[1]) +")"
                  ,(j[2]+j[3]-i[2]-i[3])/args.delta  
                  ,(j[4]-i[4])/args.delta  
                  ,(j[5]-i[5])/args.delta  
                  ,(j[6]-i[6])/args.delta  
                  ,(j[7]-i[7])/args.delta  
                  ,(j[8]-i[8])/args.delta  
                  ,(j[9]-i[9])/args.delta  
                  ,(j[10]-i[10])/args.delta  
                  ,str(i[11])+"(" + str(j[11]-i[11]) + ")" 
                  ,str(i[12])+"(" + str(j[12]-i[12]) + ")"  
                  ,str(i[14])+"(" + str(j[14]-i[14]) + ")"  
               )


        if args.stat_table_delta :
            sql = "select * from pg_stat_user_tables where schemaname = '" + args.schname +"' and relname='" + args.tabname + "'"
            # get anotehr conn and cur, if using same conn and cur seem the result set is same , not sure why
            conn2, cur2=db_conn()
            rlist1 =run_db_query(cur,sql )

            print "waiting for " + str(args.delta) + " seconds ..."
            time.sleep(args.delta) 

 
            # using the second cursor to run the same query agin 
            rlist2 =run_db_query(cur2,sql )
            # print rlist1
            # print rlist2

	    print "======================  Changes in "+ str(args.delta) + " seconds for : " + args.schname + "." + args.tabname +" ====================="
	    for (i,j) in zip(rlist1, rlist2) :
		print "relid                     | " + str(i[0])
		print "schemaname                | " + str(i[1])
		print "relname                   | " + str(i[2]) 
		print "seq_scan_delta            | {:15} rate: {:10.2f} per second".format( str(j[3] - i[3]), ( j[3] - i[3]) / args.delta  ) 
		print "seq_tup_read_delta        | {:15} rate: {:10.2f} per second".format( str(j[4] - i[4]), ( j[4] - i[4]) / args.delta  ) 
		print "idx_scan_delta            | {:15} rate: {:10.2f} per second".format( str(j[5] - i[5]), ( j[5] - i[5]) / args.delta )
		print "idx_tup_fetch_delta       | {:15} rate: {:10.2f} per second".format( str(j[6] - i[6]), ( j[6] - i[6]) / args.delta )
		print "n_tup_ins_delta           | {:15} rate: {:10.2f} per second".format( str(j[7] - i[7]), ( j[7] - i[7]) / args.delta )
		print "n_tup_upd_delta           | {:15} rate: {:10.2f} per second".format( str(j[8] - i[8]),  (j[8] - i[8]) / args.delta )
		print "n_tup_del_delta           | {:15} rate: {:10.2f} per second".format( str(j[9] - i[9]),   ( j[9] - i[9]) /   args.delta)
		print "n_tup_hot_upd_delta       | {:15} rate: {:10.2f} per second".format( str(j[10] - i[10]), ( j[10] - i[10]) / args.delta)
		print "n_live_tup_delta          | {:15} rate: {:10.2f} per second".format( str(j[11] - i[11]), ( j[11] - i[11]) / args.delta)
		print "n_dead_tup_delta          | {:15} rate: {:10.2f} per second".format( str(j[12] - i[12]), ( j[12] - i[12]) / args.delta) 
		print "n_mod_since_analyze_delta | {:15} rate: {:10.2f} per second".format( str(j[13] - i[13]), ( j[13] - i[13]) / args.delta) 
		print "-----------------------------------------------"
            # close the connection 2
     	    try :
		   cur2.close()
    	    except (Exception, psycopg2.DatabaseError) as error:
	       print(error)
	    finally:
	      if conn2 is not None:
	        conn2.close()
	        # print('Database connection closed.')
		   

        if args.stat_table :
            sql = "select * from pg_stat_user_tables where schemaname = '" + args.schname +"' and relname='" + args.tabname + "'"
            rlist =run_db_query(cur,sql )
   	    print "======================  Basic info : " + args.schname + "." + args.tabname +" ====================="
            for i in rlist:
                print "relid               | " + str(i[0])
                print "schemaname          | " + i[1]
                print "relname             | " + i[2] 
                print "seq_scan            | " + str(i[3]) 
		print "seq_tup_read        | " + str(i[4])
		print "idx_scan            | " + str(i[5])
		print "idx_tup_fetch       | " + str(i[6])
		print "n_tup_ins           | " + str(i[7])
		print "n_tup_upd           | " + str(i[8])
		print "n_tup_del           | " + str(i[9])
		print "n_tup_hot_upd       | " + str(i[10])
		print "n_live_tup          | " + str(i[11])
		print "n_dead_tup          | " + str(i[12])
		print "n_mod_since_analyze | " + str(i[13])
		print "last_vacuum         | " + str(i[14])
		print "last_autovacuum     | " + str(i[15]) 
		print "last_analyze        | " + str(i[16])
		print "last_autoanalyze    | " + str(i[17]) 
		print "vacuum_count        | " + str(i[18])
		print "autovacuum_count    | " + str(i[19])
		print "analyze_count       | " + str(i[20])
		print "autoanalyze_count   | " + str(i[21])
                print "--------------------------------------------------"


        if args.list_db :
            sql = """ 
		SELECT d.datname as "Name",
		       pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
		       pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding",
		       d.datcollate as "Collate",
		       d.datctype as "Ctype",
		       pg_catalog.array_to_string(d.datacl, E'\n') AS "Access privileges",
		       CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')
			    THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname))
			    ELSE 'No Access'
		       END as "Size",
		       t.spcname as "Tablespace",
		       pg_catalog.shobj_description(d.oid, 'pg_database') as "Description"
		FROM pg_catalog.pg_database d
		  JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid
		ORDER BY 1 """	
            rlist =run_db_query(cur,sql )
   	    print "======================  List of Databaases  ====================="
            for i in rlist:
                print "Name              | " + i[0]
                print "Owner             | " + i[1]
                print "Encoding          | " + i[2]
                print "Collate           | " + i[3]
                print "Ctype             | " + i[4]
                print "Size              | " + i[6]
                print "Tablespace        | " + str(i[7])
                print "Description       | " + str(i[8])
                print "Access privileges | " + str(i[5])
                print "--------------------------------------------------"

        if args.table_colstats :
            sql ="""
		select ps.attname, ps.inherited,ps.null_frac
		       , ps.n_distinct
		       ,ps.avg_width
		       , substr(ps.most_common_vals::text, 1,60) || '...' most_common_vals
		       , substr(ps.most_common_freqs::text, 1,60) || '...' most_common_freqs
		       ,correlation   
		       ,t.last_analyze::timestamp(0)
		from pg_stats ps join  pg_stat_all_tables  t on (ps.schemaname= t.schemaname and ps.tablename=t.relname)
		where ps.schemaname='""" + args.schname + "' and ps.tablename='" + args.tabname +"'"

            rlist =run_db_query(cur,sql )
            if len(rlist) ==  1 : 
		    print "====================== Table Column Statistics for  " +args.schname  + "." + args.tabname + " ====================="
		    for i in rlist:
			print "Column Nanme      | " +  i[0]
			print "N_Distinct        | " +  i[1]
			print "Avg_Witdth        | " +  i[2]
			print "Most Common vals  | " +  i[3]
			print "Most Common freqs | " +  i[4]
			print "Correlation       | " +  i[5]
			print "Last_Analyze      | " +  i[6]
			print "---------------------------------------"
            else : 
		    print "Table Column Statistics for  " +args.schname  + "." + args.tabname + " does not exist!" 
             
              
        if args.table_index :
            sql = """ 
              SELECT c2.relname as index_name
		       , case i.indisprimary when 't' then 'primary,' else '' end ||  
			 case i.indisunique when 't' then 'unique, ' end  || 
			 case i.indisclustered when 't' then 'clustered,' else '' end  ||
			 case  i.indisvalid  when 't' then '' else 'invalid' end as attributes 
		       , pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) as create_index_ddl
		FROM pg_catalog.pg_class c
		   , pg_catalog.pg_class c2
		   , pg_catalog.pg_index i
		     LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
		WHERE c.oid in  (
			SELECT c.oid
			FROM pg_catalog.pg_class c
			     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname ~ '^(""" + args.tabname + """)$' 
                    AND n.nspname ~ '^(""" + args.schname + """)$'
		) 
		AND c.oid = i.indrelid AND i.indexrelid = c2.oid
		ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname"""

            rlist =run_db_query(cur,sql )
   	    print "======================  List of Indexes of Table " +args.schname  + "." + args.tabname + " ====================="
            print "{:24} {:20} {:70}".format(
               'index_name', 'attributes', 'create_index_ddl' )
            print "{:21} {:20} {:70}".format(
               '------------------------', '--------------------', '----------------------------------------------------------------' )
            for i in rlist:
                print "{:24} {:20} {:70}".format( i[0], i[1], i[2] )

      
        if args.top_sql :
            sql = """
			SELECT datname, query, calls, total_time, total_time/calls as ms_per_exec,  rows, 100.0 * shared_blks_hit /
				     nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
			FROM pg_stat_statements as s inner join
			    pg_database as d on
			  d.oid =s.dbid
			where datname not in ('rdsadmin')
                 """
            if args.tsorder == 'avg' : 
                sql = sql + " ORDER BY ms_per_exec DESC limit " + str(args.limit)
            else:
                sql = sql + " ORDER BY total_time DESC limit  " + str(args.limit)

            rlist =run_db_query(cur,sql )
   	    print "======================  List of Top " + str(args.limit) + " SQLs from pg_stat_statment view  ====================="

            for i in rlist:
                print 'datname         | {:6}'.format( i[0] ) 
                print 'calls           | {:6}'.format( i[2] ) 
                print 'total time      | {:10}'.format( i[3] ) 
                print 'ms_per_execution| {:6}'.format( i[4] ) 
                print 'rows            | {:10}'.format( i[5] ) 
                print 'hit_percent     | {:10}'.format( i[6] ) 
                print 'Query           |\n ' +  i[1] 
                print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

        if args.blockers :
            sql = """
              SELECT 
		    pl.pid as blocked_pid
		    ,psa.usename as blocked_user
		    ,pl2.pid as blocking_pid
		    ,psa2.usename as blocking_user
                    ,psa2.state as blocker_state
                    ,psa2.wait_event as blocker_wait
		    ,psa.query as blocked_statement
		FROM pg_catalog.pg_locks pl
		JOIN pg_catalog.pg_stat_activity psa
		    ON pl.pid = psa.pid
		JOIN pg_catalog.pg_locks pl2
		JOIN pg_catalog.pg_stat_activity psa2
		    ON pl2.pid = psa2.pid
		    ON pl.transactionid = pl2.transactionid 
			AND pl.pid != pl2.pid
		WHERE NOT pl.granted
                order by 3
            """
            rlist =run_db_query(cur,sql )
   	    print "======================  List of waiters and blockers   ====================="
            if len(rlist) > 0 :  
		    print '{:6} {:15} {:12} {:15} {:24} {:15} {:50}'.format(
			 'pid', 'blocked_user', 'blocking_pid', 'blocking_user', "blocker_state", "blocker_wait", 'blocked_statement')
		    print '{:6} {:15} {:12} {:15} {:24} {:15} {:50}'.format(
			 '------', '---------------','------------', '-------------', '---------------------', '---------------', '-----------------------------------------------------------')
		    for i in rlist:
		        print '{:6} {:15} {:12} {:15} {:24} {:15} {:50}'.format(
			      i[0], i[1], i[2], i[3], i[4], i[5], i[6] )
            else :  
                   print "No blockers and waiters"


        if args.display_roles :
            sql = """
	      SELECT r.rolname,
		      case r.rolsuper when 't' then 'Superuser,' else '' end  || 
		      -- case r.rolinherit when 't' then 'Inherit,' else '' end  ||
		      case r.rolcreaterole when 't' then 'Create Role,' else '' end || 
		      case r.rolcreatedb when 't' then 'Create DB,' else '' end ||
		      case r.rolcanlogin  when 'f' then 'Cannot Login,' else '' end|| 
		      case r.rolreplication when 't' then 'Replication' else '' end
		      as attribute,  
		      -- r.rolconnlimit, 
		      -- r.rolvaliduntil,
		      -- , r.rolbypassrls
		      ARRAY(SELECT b.rolname
				FROM pg_catalog.pg_auth_members m
				JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
				WHERE m.member = r.oid) as memberof
		      , pg_catalog.shobj_description(r.oid, 'pg_authid') AS description
		FROM pg_catalog.pg_roles r
		WHERE r.rolname !~ '^pg_'
		ORDER BY 1
              """
            rlist =run_db_query(cur,sql )
            print "======================  List of roles   ====================="
            print '{:17} {:50} {:34} {:20}'.format(
                 'Role name', 'Attributes', 'Member of', 'Description')
            print '{:17} {:50} {:34} {:20}'.format(
                 '--------------------', '-------------------------------------------------', '--------------------------', '-------------------')
            for i in rlist:
                print '{:17} {:50} {:34} {:20}'.format(
                      i[0], i[1], i[2], i[3] )

        if args.bloat_size :
            sql ="""
		/* WARNING: executed with a non-superuser role, the query inspect only tables you are granted to read.
		* This query is compatible with PostgreSQL 9.0 and more
		*/
		select current_database, schemaname, tblname, real_size, extra_size, round(extra_ratio), fillfactor, bloat_size, round(bloat_ratio), is_na
                from
		(
		SELECT current_database(), schemaname, tblname, bs*tblpages AS real_size,
		  (tblpages-est_tblpages)*bs AS extra_size,
		  CASE WHEN tblpages - est_tblpages > 0
		    THEN 100 * (tblpages - est_tblpages)/tblpages::float
		    ELSE 0
		  END AS extra_ratio, fillfactor,
		  CASE WHEN tblpages - est_tblpages_ff > 0
		    THEN (tblpages-est_tblpages_ff)*bs
		    ELSE 0
		  END AS bloat_size,
		  CASE WHEN tblpages - est_tblpages_ff > 0
		    THEN 100 * (tblpages - est_tblpages_ff)/tblpages::float
		    ELSE 0
		  END AS bloat_ratio, is_na
		  -- , (pst).free_percent + (pst).dead_tuple_percent AS real_frag
		FROM (
		  SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages,
		    ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff,
		    tblpages, fillfactor, bs, tblid, schemaname, tblname, heappages, toastpages, is_na
		    -- , stattuple.pgstattuple(tblid) AS pst
		  FROM (
		    SELECT
		      ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
			- CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
			- CASE WHEN ceil(tpl_data_size)::int%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%ma END
		      ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages,
		      toastpages, reltuples, toasttuples, bs, page_hdr, tblid, schemaname, tblname, fillfactor, is_na
		    FROM (
		      SELECT
			tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples,
			tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,
			coalesce(toast.reltuples, 0) AS toasttuples,
			coalesce(substring(
			  array_to_string(tbl.reloptions, ' ')
			  FROM 'fillfactor=([0-9]+)')::smallint, 100) AS fillfactor,
			current_setting('block_size')::numeric AS bs,
			CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
			24 AS page_hdr,
			23 + CASE WHEN MAX(coalesce(null_frac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END
			  + CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size,
			sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) AS tpl_data_size,
			bool_or(att.atttypid = 'pg_catalog.name'::regtype)
			  OR count(att.attname) <> count(s.attname) AS is_na
		      FROM pg_attribute AS att
			JOIN pg_class AS tbl ON att.attrelid = tbl.oid
			JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
			LEFT JOIN pg_stats AS s ON s.schemaname=ns.nspname
			  AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
			LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid
		      WHERE att.attnum > 0 AND NOT att.attisdropped
			AND tbl.relkind = 'r'
		      GROUP BY 1,2,3,4,5,6,7,8,9,10, tbl.relhasoids
		      ORDER BY 2,3
		    ) AS s
		  ) AS s2
		) AS s3
		) AS S4
		order by 8 desc limit """  + str(args.limit)

            rlist =run_db_query(cur,sql )
            print "======================  Top " +str(args.limit) + " Bloat Table By Size   ====================="
            print '{:16} {:10} {:25} {:12} {:14} {:12} {:12}  {:12}  {:12}  {:5}'.format(
                 'current_database', 'schemaname', 'tblname', 'real_size', 'extra_size', 'extra_ratio', 'fillfactor', 'bloat_size', 'bloat_ratio', 'is_na')
            print '{:16} {:10} {:25} {:12} {:14} {:12} {:12}  {:12}  {:12}  {:5}'.format(
                 '-----------', '----------', '--------------------'
               , '------------' , '------------' , '------------' , '------------' , '------------' , '------------' , '-----')
            for i in rlist:
                print '{:16} {:10} {:25} {:12} {:14} {:12} {:12}  {:12}  {:12}  {:5}'.format(
                      i[0], i[1], i[2], i[3] ,i[4], i[5], i[6], i[7] ,i[8], i[9])

        if args.bloat_ratio :
            sql ="""
		/* WARNING: executed with a non-superuser role, the query inspect only tables you are granted to read.
		* This query is compatible with PostgreSQL 9.0 and more
		*/
		select 
                 current_database, schemaname, tblname, real_size, extra_size, round(extra_ratio), fillfactor, bloat_size, round(bloat_ratio), is_na
                from
		(
		SELECT current_database(), schemaname, tblname, bs*tblpages AS real_size,
		  (tblpages-est_tblpages)*bs AS extra_size,
		  CASE WHEN tblpages - est_tblpages > 0
		    THEN 100 * (tblpages - est_tblpages)/tblpages::float
		    ELSE 0
		  END AS extra_ratio, fillfactor,
		  CASE WHEN tblpages - est_tblpages_ff > 0
		    THEN (tblpages-est_tblpages_ff)*bs
		    ELSE 0
		  END AS bloat_size,
		  CASE WHEN tblpages - est_tblpages_ff > 0
		    THEN 100 * (tblpages - est_tblpages_ff)/tblpages::float
		    ELSE 0
		  END AS bloat_ratio, is_na
		  -- , (pst).free_percent + (pst).dead_tuple_percent AS real_frag
		FROM (
		  SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages,
		    ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff,
		    tblpages, fillfactor, bs, tblid, schemaname, tblname, heappages, toastpages, is_na
		    -- , stattuple.pgstattuple(tblid) AS pst
		  FROM (
		    SELECT
		      ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
			- CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
			- CASE WHEN ceil(tpl_data_size)::int%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%ma END
		      ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages,
		      toastpages, reltuples, toasttuples, bs, page_hdr, tblid, schemaname, tblname, fillfactor, is_na
		    FROM (
		      SELECT
			tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples,
			tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,
			coalesce(toast.reltuples, 0) AS toasttuples,
			coalesce(substring(
			  array_to_string(tbl.reloptions, ' ')
			  FROM 'fillfactor=([0-9]+)')::smallint, 100) AS fillfactor,
			current_setting('block_size')::numeric AS bs,
			CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
			24 AS page_hdr,
			23 + CASE WHEN MAX(coalesce(null_frac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END
			  + CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size,
			sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) AS tpl_data_size,
			bool_or(att.atttypid = 'pg_catalog.name'::regtype)
			  OR count(att.attname) <> count(s.attname) AS is_na
		      FROM pg_attribute AS att
			JOIN pg_class AS tbl ON att.attrelid = tbl.oid
			JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
			LEFT JOIN pg_stats AS s ON s.schemaname=ns.nspname
			  AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
			LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid
		      WHERE att.attnum > 0 AND NOT att.attisdropped
			AND tbl.relkind = 'r'
		      GROUP BY 1,2,3,4,5,6,7,8,9,10, tbl.relhasoids
		      ORDER BY 2,3
		    ) AS s
		  ) AS s2
		) AS s3
		) AS S4
		order by 9 desc limit """ + str(args.limit)
            rlist =run_db_query(cur,sql )
            print "======================  Top " + str(args.limit) + " Bloat Table By Ratio   ====================="
            print '{:16} {:19} {:25} {:12} {:14} {:12} {:12}  {:12}  {:12}  {:5}'.format(
                 'current_database', 'schemaname', 'tblname', 'real_size', 'extra_size', 'extra_ratio', 'fillfactor', 'bloat_size', 'bloat_ratio', 'is_na')
            print '{:16} {:19} {:25} {:12} {:14} {:12} {:12}  {:12}  {:12}  {:5}'.format(
                 '-----------', '----------', '--------------------'
               , '------------' , '------------' , '------------' , '------------' , '------------' , '------------' , '-----')
            for i in rlist:
                 print '{:16} {:19} {:25} {:12} {:14} {:12} {:12}  {:12}  {:12}  {:5}'.format(
                      i[0], i[1], i[2], i[3] ,i[4], i[5], i[6], i[7] ,i[8], i[9])


        if args.top_objects :
            sql ="""
                   SELECT n.nspname || '.' ||  relname AS objectname,
			   relkind AS objecttype,
			   reltuples::int AS "#entries", pg_size_pretty(relpages::bigint*8*1024) AS size
			   FROM pg_class C
			     left join pg_namespace N on (N.oid = C.relnamespace)
			   WHERE relpages >= 8
			   ORDER BY relpages DESC
			limit """  + str(args.limit) 
            rlist =run_db_query(cur,sql )
            print "======================  Top " + str(args.limit) +" Objects by Size   ====================="
            print "{:35} {:4} {:15}  {:15}".format(
                  'objectname', 'objecttype', '#entries', 'size' )
            print "{:35} {:4} {:15}  {:15}".format(
                  '------------------------------', '----', '--------------', '---------------' )
            for i in rlist:
                 print "{:35} {:4} {:15}  {:15}".format(
                      i[0], i[1], i[2], i[3] )
  

        if args.hot_tables :
            sql ="""
		select schemaname, relname, n_live_tup, n_tup_upd, n_tup_del, n_dead_tup, last_vacuum::timestamp(0),
		last_autovacuum::timestamp(0),
		last_analyze::timestamp(0), last_autoanalyze::timestamp(0)
		from pg_stat_user_tables
		order by n_dead_tup desc  limit """ + str(args.limit)
            rlist =run_db_query(cur,sql )
            print "======================  Top " + str(args.limit) + " Hot Tables from DML  ====================="
            print '{:15} {:30} {:10} {:10} {:10} {:10} {:20} {:20} {:20} {:20}'.format(
              'schemaname', 'relname', 'n_live_tup', 'n_tup_upd' , 'n_tup_del', 'n_dead_tup', 'last_vacuum', 'last_autovacuum','last_analyze','last_autoanlyze' )
            print '{:15} {:30} {:10} {:10} {:10} {:10} {:20} {:20} {:20} {:20}'.format(
                      '----------------'
                     ,'----------------'
                     ,'---------'
                     ,'---------'
                     ,'---------'
                     ,'---------'
                     ,'--------------------'
                     ,'--------------------'
                     ,'--------------------'
                     ,'--------------------'
                     )
            for i in rlist:
                  print '{:15} {:30} {:10} {:10} {:10} {:10} {:20} {:20} {:20} {:20}'.format(
                      str(i[0]) ,str(i[1]) ,str(i[2]) ,str(i[3]) ,str(i[4]) ,str(i[5]) ,str(i[6]) ,str(i[7]) ,str(i[8]) ,str(i[9])
                  )

         
        if args.db_stats_per_sec :
            sql ="""
		select datname, stats_reset ,round(((xact_commit + xact_rollback) /EXTRACT(EPOCH FROM (now() - stats_reset)))::numeric,1) as xtrans_per_sec
		,round((tup_returned /EXTRACT(EPOCH FROM (now() - stats_reset)))::numeric,1) as tup_returned_per_sec
		,round((tup_fetched /EXTRACT(EPOCH FROM (now() - stats_reset)))::numeric,1) as tup_fetched_per_sec
		,round((tup_inserted /EXTRACT(EPOCH FROM (now() - stats_reset)))::numeric,1) as tup_inserted_per_sec
		,round((tup_updated /EXTRACT(EPOCH FROM (now() - stats_reset)))::numeric,1) as tup_updated_per_sec
		,round((tup_deleted /EXTRACT(EPOCH FROM (now() - stats_reset)))::numeric,1) as tup_deleted_per_sec
		from pg_stat_database
		where stats_reset is not null
		order by 5 desc
                  """ 
            rlist =run_db_query(cur,sql )
            print "======================  Per-Second Database Statistics Since Last Reset ====================="
            print '{:20} {:40} {:15} {:15} {:15} {:15} {:15} {:15}'.format('datname', 'stats_reset', 'trans_per_sec', 'tup_returned_ps', 'tup_fetched_ps', 'tup_inserted_ps', 'tup_updated_ps', 'tup_deleted_ps')
            print '{:20} {:40} {:15} {:15} {:15} {:15} {:15} {:15}'.format( '--------------------' ,'--------------------' ,'---------------' ,
                                     '---------------' ,'---------------' ,'---------------' ,'---------------' ,'---------------')
            for i in rlist:
                print '{:20} {:40} {:15} {:15} {:15} {:15} {:15} {:15}'.format( i[0] , str(i[1]) , str(i[2]) , str(i[3]) , str(i[4]) , str(i[5]) , str(i[6]), str(i[7])   )



        if args.db_stats :

            sql ="""
		   select datname, numbackends, xact_commit,
		   tup_returned, tup_fetched, tup_inserted,
		   tup_updated, tup_deleted,
		   pg_size_pretty(pg_database_size(datname))
		   FROM pg_stat_database
		"""
            rlist =run_db_query(cur,sql )
            print "======================  Database Statistics ====================="
            print '{:15} {:15} {:15} {:15} {:15} {:15} {:15} {:15}  {:15}'.format('datname', 'numbackends', 'xact_commit', 'tup_returned', 'tup_fetched', 'tup_inserted', 'tup_updated', 'tup_deleted',  'pg_size_pretty')
            print '{:15} {:15} {:15} {:15} {:15} {:15} {:15} {:15} {:15}  '.format(
                 '---------------', '---------------', '---------------', '---------------', '---------------', 
                 '---------------', '---------------', '---------------',  '---------------' )
            for i in rlist:
                 print '{:15} {:15} {:15} {:15} {:15} {:15}  {:15}  {:15}  {:15}'.format(
                        i[0] , i[1], i[2], i[3] ,i[4] ,i[5] , i[6] , i[7] , i[8]  )

        if args.running_query :
           
	    sql ="""
		select DATE_PART('minute',(now() - query_start)) as duration,query,state,usename
		from pg_stat_activity where state != 'idle'
		and usename !='rdsadmin'
		order by duration desc
		limit """ + str(args.limit)

            rlist =run_db_query(cur, sql)

            print "======================  Current  Ruinng Queires Top " + str(args.limit) + " by Duration ====================="
            for i in rlist:
                print "Duration: " +  str(i[0]) + " min"  
                print "Query: " +  str(i[1]) 
                print "State: " +  str(i[2]) 
                print "Username: " +  str(i[3]) 
                print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

        if args.db_size :
            if args.dbname :
              rlist =db_size(cur, args.dbname)
            else: 
	      rlist = db_size(cur,'postgres')
            print "===================== DB Size  ==================================="
            for i in rlist:
               if args.dbname :
                  print "DB Name: " + args.dbname + " Size: " + str(i[0])
               else :
                  print "DB Name (default): postgres" + " Size: " + str(i[0])
                  print "hint: using --dbname option to specify a db"


        if args.wait_event :

            sql =""" select state, concat(wait_event_type,' - ', wait_event) as waitevent, count(*) from pg_stat_activity  where pid != pg_backend_pid() and state !='idle' group by state, waitevent order by 3 desc """ 
            rlist = run_db_query(cur,sql )
            print "======================================================= "
            print "~~~~~~~~~~~~~ Wait event count  ~~~~~~~~~~~~~~~~~"
            print "======================================================= "
            print '{:40} {:20} {:10} '.format('STATE', 'WAIT EVENT', 'COUNT')
            print '{:40} {:20} {:10} '.format('----------------------------------------', '--------------------', '----------')
            for i in rlist:
                print '{:40} {:20} {:10} '.format(str(i[0]), str(i[1]), str(i[2]))
            print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

        if args.conn_info :

            sql =""" SELECT concat(now()::timestamp(0) , ' : ', current_user, '@', current_database(), ' -  SERVER_IP:', inet_server_addr(), ' PORT: ', inet_server_port()) as conninfo """
            conn_info_list = run_db_query(cur, sql)
            print "======================================================= "
            print "~~~~~~~~~~~~~ Connecting session info  ~~~~~~~~~~~~~~~~~"
            print "======================================================= "
            for i in conn_info_list:
                print i[0]
                print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

        if args.version :
            sql =""" SELECT  version() """
            pg_version_list = run_db_query(cur, sql)
            print "======================================================= "
            print "~~~~~~~~~~~~~  PostgreSQL Version   ~~~~~~~~~~~~~~~~~"
            print "======================================================= "
            for i in pg_version_list:
                print i[0]

        if args.active_session :
            if args.no_idle_in_transaction:
		    sql ="""
			select 
			       pid,
			       usename, 
			       datname,
			       application_name,
			       client_hostname,
			       client_addr,
			       state,
			       wait_event,
			       now() - state_change as runtime,
			       query
			from pg_stat_activity
			where state not in ('idle', 'idle in transaction')
			  and pid != pg_backend_pid()
			order by usename, state
			"""
            else:
		    sql ="""
			select 
			       pid,
			       usename, 
			       datname,
			       application_name,
			       client_hostname,
			       client_addr,
			       state,
			       wait_event,
			       now() - state_change as runtime,
			       query
			from pg_stat_activity
			where state != 'idle'
			  and pid != pg_backend_pid()
			order by usename, state
			"""

            active_session_list = run_db_query(cur,sql)
            print "======================================================= "
            print "~~~~~~~~~~~~~~ Active sessions ~~~~~~~~~~~~~~~~~~~~~~~~~"
            print "======================================================= "
            for i in active_session_list:
                 print "PID: " + str(i[0]) + " USER: " + str(i[1]) + ' DB: ' +  str(i[2]) 
                 print "APP NAME: " + str(i[3]) + " CLIENT HOST: " + str(i[4])+  " CLIENT ADDR: " + str(i[5])
                 print 'STATE: ' + str(i[6]) + ' WAIT EVENT: ' +  str(i[7])
                 print 'RUN TIME: ' + str(i[8])
                 print 'QUERY: \n' + str(i[9])
                 print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

            print "Total # Active Sessions: " + str(len(active_session_list))

        if args.pg_settings :
           sql ="""
		SELECT category, string_agg(name || '=' || setting, E'\n' ORDER BY name) As settings
		 FROM pg_settings
		    GROUP BY category
		    ORDER BY category
		"""
           pg_setting_list = run_db_query(cur,sql)
           print "======================================================= "
           print "~~~~~~~~~~~~~~ PG  Setting ~~~~~~~~~~~~~~~~~~~~~~~~~"
           print "======================================================= "
           for i in pg_setting_list:
               print "*** Category: " +  i[0]
               print  i[1]
               print "\n"


        # close the communication with the PostgreSQL
        try :
           cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
               print(error)
        finally:
          if conn is not None:
            conn.close()
            # print('Database connection closed.')

