"""
Purpose: Move Data between Redshift Platforms.
Developer: Kotesh Mukkamala.
Initial Draft: 07/13/2017
History of Changes:
Usage:
    print "python rsdatamover.py <configuration> "
    print "example python rsdatamover.py /Users/kmukkamala/git/dwh/Common/dwh_creds/rsdatamover.ctrl"
    print "rsdatamover.ctrl can be configured to do all tables in the schema or a list of tables.
    print  "tableName": ["km_testtable1","km_testtable3"]
    print   "tableName": "all"

"""
import json
import sys
import logging
import numpy as np
import os
import psycopg2
import time
import boto
from boto import kms, s3
import datetime
import smtplib
from email.mime.text import MIMEText

#path ="/Users/kmukkamala/git/dwh/Common/dwh_creds/rsdatamover.ctrl"

def usage():
    print "Redshift Data Mover Utility. Incorrect arguments passed."
    print "Usage Error: Find usage mechanism below along with an example"
    print "python rsdatamover.py <configuration> "
    print "example python rsdatamover.py /Users/kmukkamala/git/dwh/Common/dwh_creds/rsdatamover.ctrl"
    sys.exit(-1)


def getConfig(path):
    global config
    if path.startswith("s3://"):
        # download the configuration from s3
        s3Info = tokeniseS3Path(path)
        bucket = s3Client.get_bucket(s3Info[0])
        key = bucket.get_key(s3Info[1])
        configContents = key.get_contents_as_string()
        config = json.loads(configContents)
    else:
      with open(path) as f:
            config = json.load(f)


def make_conn(conn_string):
  return psycopg2.connect(conn_string)

def close_conn(conn):
  conn.close()
  #print "Closing redshift database connection"

def execute_sql(conn,sql_text):
  try:
    cur=conn.cursor()
    cur.execute(sql_text)
    if cur.rowcount>0:
      results=cur.fetchall()
      cur.close()
      return results
    else:
      cur.close()
  except psycopg2.Error, e:
    try:
       print "Redshift Error [%d]: %s" % (e.args[0], e.args[1])
       logging.error("Redshift Error [%d]: %s" % (e.args[0], e.args[1]))
       return False
    except IndexError:
      print "Redshift Error: %s" % str(e)
      logging.error("Redshift Error: %s" % str(e))
      return False
    except TypeError, e:
      print(e)
      logging.error(e)
      return False
    except ValueError, e:
      print(e)
      logging.error(e)
      return False
    finally:
      cur.close()

def check_table_exists(conn,schemaname,tablename):
  sql_text="select count(*) From pg_catalog.pg_tables where schemaname='" + schemaname + "' and tablename='"+ tablename +"'"
  sql_results=execute_sql(conn,sql_text)
  if return_first_value(sql_results)> 0:
    return True
  else:
    return False

def gen_table_ddl(conn,src_schema,src_table,tgt_schema,tgt_table):
  sql_text="select 'create table  " + tgt_schema +"." + tgt_table +  " ('||cp.coldef || nvl(d.distkey,'') || nvl((select ' sortkey(' ||substr(array_to_string(array( select ','||cast(column_name as varchar(100))  as str from (select column_name from information_schema.columns col where  col.table_schema= tm.schemaname and col.table_name=tm.tablename) c2 join (select attrelid as tableid, attname as colname, attsortkeyord as sort_col_order from pg_attribute pa where pa.attnum > 0  AND NOT pa.attisdropped AND pa.attsortkeyord > 0) st on tm.tableid=st.tableid and c2.column_name=st.colname   order by sort_col_order),''),2,10000) || ')'),'') ||';' from (SELECT substring(n.nspname,1,100) as schemaname, substring(c.relname,1,100) as tablename, c.oid as tableid FROM pg_namespace n, pg_class c WHERE n.oid = c.relnamespace AND nspname ='" + src_schema + "' and relname='" + src_table +"') tm join (select substr(str,(charindex('QQQ',str)+3),(charindex('ZZZ',str))-(charindex('QQQ',str)+3)) as tableid,substr(replace(replace(str,'ZZZ',''),'QQQ'||substr(str,(charindex('QQQ',str)+3),(charindex('ZZZ',str))-(charindex('QQQ',str)+3)),''),2,10000) as coldef from( select array_to_string(array(SELECT  'QQQ'||cast(t.tableid as varchar(10))||'ZZZ'|| ','||column_name||' '|| decode(udt_name,'bpchar','char',udt_name) || decode(character_maximum_length,null,'', '('||cast(character_maximum_length as varchar(9))||')'   ) || decode(substr(column_default,2,8),'identity','',null,'',' default '||column_default||' ') || decode(is_nullable,'YES',' NULL ','NO',' NOT NULL ') || decode(substr(column_default,2,8),'identity',' identity('||substr(column_default,(charindex('''',column_default)+1), (length(column_default)-charindex('''',reverse(column_default))-charindex('''',column_default)   ) )  ||') ', '') as str from(select cast(t.tableid as int), cast(table_schema as varchar(100)), cast(table_name as varchar(100)), cast(column_name as varchar(100)), cast(ordinal_position as int), cast(column_default as varchar(100)), cast(is_nullable as varchar(20)) , cast(udt_name as varchar(50))  ,cast(character_maximum_length as int), sort_col_order  , decode(d.colname,null,0,1) dist_key from (select * from information_schema.columns c where  c.table_schema= t.schemaname and c.table_name=t.tablename) c left join (select attrelid as tableid, attname as colname, attsortkeyord as sort_col_order from pg_attribute a where a.attnum > 0  AND NOT a.attisdropped AND a.attsortkeyord > 0) s on t.tableid=s.tableid and c.column_name=s.colname left join (select attrelid as tableid, attname as colname from pg_attribute a where a.attnum > 0 AND NOT a.attisdropped  AND a.attisdistkey = 't') d on t.tableid=d.tableid and c.column_name=d.colname order by ordinal_position) ci ), '') as str from (SELECT substring(n.nspname,1,100) as schemaname, substring(c.relname,1,100) as tablename, c.oid as tableid FROM pg_namespace n, pg_class c WHERE n.oid = c.relnamespace AND nspname ='" + src_schema + "' and relname='" + src_table + "' ) t)) cp on tm.tableid=cp.tableid left join(  select ')' ||' distkey('|| cast(column_name as varchar(100)) ||')'  as distkey, t.tableid from information_schema.columns c join (SELECT substring(n.nspname,1,100) as schemaname, substring(c.relname,1,100) as tablename, c.oid as tableid FROM pg_namespace n, pg_class c WHERE n.oid = c.relnamespace AND nspname ='" + src_schema + "' and relname='" + src_table + "') t on c.table_schema= t.schemaname and c.table_name=t.tablename join (select attrelid as tableid, attname as colname from pg_attribute a where a.attnum > 0 AND NOT a.attisdropped  AND a.attisdistkey = 't') d on t.tableid=d.tableid and c.column_name=d.colname) d on tm.tableid=d.tableid"

  sql_results=execute_sql(conn,sql_text)
  return return_first_value(sql_results)

def check_row_count(conn,sql_text):
    sql_results=execute_sql(conn,sql_text)
    return return_row_count(sql_results)

def return_row_count(results):
  for i in results:
    return i[0]

def return_first_value(results):
  for i in results:
    return i[0]


def unload_module(src_conn_string,src_schema,src_table,src_predicate,s3_location,aws_access_key_id,aws_secret_access_key,delimited_by):
  src_conn=make_conn(src_conn_string)
  logging.info("Checking for source table " + src_schema + "." + src_table + " existence...")
  unload_rowcount=0
  if not check_table_exists(src_conn,src_schema,src_table):
    return unload_rowcount
    logging.info("Source Table " + src_schema + "." + src_table + " doesn't exist. Skipping...")
  else:
    logging.info("Source Table exists. Checking row count now...")
    unload_sql_rowcount_text="select count(*) from " + src_schema + "." + src_table + " " + src_predicate
    unload_rowcount=check_row_count(src_conn,unload_sql_rowcount_text)
    if unload_rowcount<1:
      logging.info("No records qualify for table  " + src_schema + "." + src_table + ". Skipping...")
      close_conn(src_conn)
    else:
      logging.info("Unloading " + str(unload_rowcount) + " records from " + src_schema + "." + src_table + " on to S3...")
      unload_sql_text="select * from " + src_schema + "." + src_table + " " + src_predicate
      unload_sql_text=unload_sql_text.replace("'",r"''")
      sql_text= "unload ('"+ unload_sql_text + "') to '" + s3_location + src_table + "' credentials 'aws_access_key_id=" + aws_access_key_id + ";aws_secret_access_key=" + aws_secret_access_key + "' delimiter as '" + delimited_by + "' addquotes escape manifest ALLOWOVERWRITE GZIP;"
      execute_sql(src_conn,sql_text)
      close_conn(src_conn)
    return unload_rowcount

def copy_module(src_conn_string,tgt_conn_string,src_schema,src_table,tgt_schema,tgt_table,s3_location,aws_access_key_id,aws_secret_access_key,delimited_by,errorLimit):
  copy_status=True
  tgt_conn=make_conn(tgt_conn_string)
  tgt_conn.set_isolation_level(0)
  logging.info("Checking for target table existence ")
  if not check_table_exists(tgt_conn,tgt_schema,tgt_table):
    logging.info("Target Table doesn't exist, creating one now")
    logging.info("Extracting the source ddl..")
    src_conn=make_conn(src_conn_string)
    tgt_table_ddl=gen_table_ddl(src_conn,src_schema,src_table,tgt_schema,tgt_table)
    close_conn(src_conn)
    execute_sql(tgt_conn,tgt_table_ddl)
    logging.info("Target Table created and ready to be loaded!!")

  sql_text="COPY " + tgt_schema + "." + tgt_table + " from '" + s3_location + src_table + "manifest' credentials 'aws_access_key_id=" + aws_access_key_id + ";aws_secret_access_key=" + aws_secret_access_key + "' ACCEPTINVCHARS dateformat 'auto' gzip manifest maxerror " + str(errorLimit) + " delimiter '" + delimited_by + "' removequotes escape"
  logging.info("Begining to load data now to " + tgt_schema + "." + tgt_table + ". This could take a few minutes...")
  if execute_sql(tgt_conn,sql_text)==False:
    copy_status=False
  else:
    logging.info("Data Load is complete. Proceeding to Analyze and vacuum...")
    sql_text="Analyze " + tgt_schema + "." + tgt_table
    execute_sql(tgt_conn,sql_text)
    logging.info( "Analyze Done. Vacuuming now...")
    sql_text="Vacuum " + tgt_schema + "." + tgt_table
    execute_sql(tgt_conn,sql_text)
  close_conn(tgt_conn)
  logging.info("Proceeding to S3 clean up...")
  return copy_status

def tokeniseS3Path(path):
    pathElements = path.split('/')
    bucketName = pathElements[2]
    prefix = "/".join(pathElements[3:])

    return (bucketName, prefix)

def s3Delete(stagingPath,aws_access_key_id,aws_secret_access_key):
    logging.info( "Cleaning up S3 Data Staging Location %s" % (stagingPath))
    s3Info = tokeniseS3Path(stagingPath)
    conn=boto.connect_s3(aws_access_key_id,aws_secret_access_key)
    stagingBucket = conn.get_bucket(s3Info[0])

    for key in stagingBucket.list(s3Info[1]):
         stagingBucket.delete_key(key)
    logging.info("Cleanup of Stage Area is complete. Exiting the program...")

def email_module(to_email,email,password_text,filename):
  if to_email <> "":
    from_email="dataops@nerdwallet.com"
    fp=open(filename,'rb')
    msg = MIMEText(fp.read())
    fp.close()
    msg['Subject'] = "Redshift Data Mover Job Status " + filename
    msg['From'] = from_email
    msg['To'] = to_email
    s = smtplib.SMTP(host='smtp.gmail.com', port=587)
    s.starttls()
    s.login(email,password_text)
    s.sendmail(from_email, to_email, msg.as_string())
    s.quit()
    return True
  else:
    return False

def main(args):
    if len(args) != 2:
      usage()

    # load the configuration
    getConfig(args[1])

    nowString = "{:%Y-%m-%d_%H-%M-%S}".format(datetime.datetime.now())

    if config['logging']['logDir']=="":
	log_dir=os.getcwd() + "/"
    else:
        log_dir=config['logging']['logDir']
        if log_dir[-1]<>"/":
          log_dir = log_dir + "/"
    
    # parse options
    s3_location=config['s3Staging']['s3_location'] + nowString + "/"
    
    if not s3_location.startswith("s3://"):
        print "s3_location must be a path to S3"
        sys.exit(-1)

    dataStagingRegion = None
    if 'region' in config["s3Staging"]:
        dataStagingRegion = config["s3Staging"]['region']


    if 'aws_iam_role' in config["s3Staging"]:
        accessRole = config['s3Staging']['aws_iam_role']
        s3_access_credentials = "aws_iam_role=%s" % accessRole
    else:
        aws_access_key_id = config['s3Staging']['aws_access_key_id']
        aws_secret_access_key = config['s3Staging']['aws_secret_access_key']

    deleteOnSuccess = config['s3Staging']['deleteOnSuccess']

    # source from which to export data
    srcConfig = config['unloadSource']

    src_host = srcConfig['clusterEndpoint']
    src_port = srcConfig['clusterPort']
    src_db = srcConfig['db']
    src_tablelist=srcConfig['tablelist']
    delimited_by= srcConfig['delimiter']
    src_user = srcConfig['connectUser']
    src_password=srcConfig['connectPwd']
    log_file =log_dir + "rsdatamover." + src_user + "." + nowString +".log"
    to_email= config['logging']['notifyEmail'] 
    email=config['logging']['senderEmail'] 
    password_txt=config['logging']['password']
    src_conn_string="dbname='" + src_db + "' host='" + src_host + "' port='" + str(src_port) +"' user= '" + src_user + "' password= '" + src_password +"'"

    # target to which we'll import data
    destConfig = config['copyTarget']
    dest_host = destConfig['clusterEndpoint']
    dest_port = destConfig['clusterPort']
    dest_db = destConfig['db']
    dest_user = destConfig['connectUser']
    dest_password= destConfig['connectPwd']
    errorLimit=destConfig['errorLimit']
    tgt_conn_string = "dbname='" + dest_db + "' host='" + dest_host + "' port='" + str(dest_port) +"' user= '" + dest_user + "' password= '" + dest_password +"'"

    tablelist = []
    with open(src_tablelist, "rb") as fp:
      for i in fp.readlines()[1:]:
        tmp = i.split("|")
        try:
            tablelist.append((tmp[0].rstrip("\n"), tmp[1].rstrip("\n"),tmp[2].rstrip("\n")))
        except:pass

    logging.basicConfig(filename=log_file,level=logging.INFO)
    logging.info("Starting the move..")    

    for item in tablelist:
      start_time="{:%Y-%m-%d_%H:%M:%S}".format(datetime.datetime.now())
      logging.info("Moving table " + item[0] + "...")
      
      src_schema=item[0].split(".")[0]
      src_table=item[0].split(".")[1]
      src_predicate=item[1]
      if item[2]<>"":
        tgt_schema=item[2].split(".")[0]
        tgt_table=item[2].split(".")[1]
      else:
        tgt_schema=src_schema
        tgt_table=src_table
      unload_rowcount=unload_module(src_conn_string,src_schema,src_table,src_predicate,s3_location,aws_access_key_id,aws_secret_access_key,delimited_by)
      if unload_rowcount > 0:
        copy_status= copy_module(src_conn_string,tgt_conn_string,src_schema,src_table,tgt_schema,tgt_table,s3_location,aws_access_key_id,aws_secret_access_key,delimited_by,errorLimit)
        if 'true' == deleteOnSuccess.lower():
          s3Delete(s3_location,aws_access_key_id,aws_secret_access_key)
        if copy_status== False:
          status="Failed. Check the log file for errors"
          unload_rowcount=0
        else:
          status="Complete..."
      else:
	  status="Skipped..."
      end_time="{:%Y-%m-%d_%H:%M:%S}".format(datetime.datetime.now())
      
      print "{"
      print "   Src Table: " + item[0]
      print "   Tgt Table: " + tgt_schema + "." + tgt_table
      print "   Total Records: " + str(unload_rowcount)
      print "   Start Time: " + start_time
      print "   Finish Time: " + end_time
      print "   Status: " + status
      print "}"
      logging.info("Src Table: " + item[0] + ", Tgt Table: " + tgt_schema + "." + tgt_table + " Total Records: " + str(unload_rowcount) + ", Start Time: " + start_time + ", Finish Time: " + end_time + "  Status: " + status)
      
    if email_module(to_email,email,password_txt,log_file)==True:
      print "Check details in the email."
    else:
     print "Check log file " +log_file +" for details."


if __name__ == "__main__":
    main(sys.argv)
