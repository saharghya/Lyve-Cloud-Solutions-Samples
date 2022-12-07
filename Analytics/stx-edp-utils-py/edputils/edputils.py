from requests.auth import HTTPBasicAuth
from pyhive import presto
from cloudpathlib import CloudPath
import requests
import logging
from contextlib import closing
import pyarrow.dataset as ds
import boto3
import pandas as pd
from pyarrow import fs
import pyarrow.orc as po
import pyarrow as pa
import random

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.WARNING, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()


class edputils:
    def get_trino_connection(user,password,host='enterprisehadoopuser.seagate.com', port=8081):
        requests.packages.urllib3.disable_warnings()
        req_kw = {'auth': HTTPBasicAuth(user, password),'verify':False}
        
        conn = presto.connect(
            host=host,
            port=int(port), 
            protocol='https',
            catalog='hive',
            username=user,
            requests_kwargs=req_kw
        )
        return conn
    
    def close_trino_connection(connection):
        connection.close()
        logger.warning('Connection closed')
    
    def __drop_table(table_name_qualified, conn):
        with closing(conn.cursor()) as cur:
            drop_query = "drop table if exists {}".format(table_name_qualified)
            cur.execute(drop_query)
            cur.fetchall()  
        
    def __create_table_from_query(user_schema, select_query, conn, file_format='ORC', compression='GZIP'):
        logger.warning('Query Started')
        with closing(conn.cursor()) as cur:
            cur.execute("set session hive.compression_codec='{}'".format(compression))
            cur.fetchall()
            cur.execute("set session hive.collect_column_statistics_on_write=false")
            cur.fetchall()
            
            tmp_table_qualified = "{0}.edputils_{1}".format(user_schema, random.randint(100000000000,999999999999))
            create_query = "create table {} with (format = '{}') as {}".format(tmp_table_qualified, file_format, select_query)
            cur.execute(create_query)
            location = cur.fetchall()
            logger.warning('Query Completed')
            return tmp_table_qualified
    
    def __get_table_location(table_name_qualified, conn):
        location_query = "select url_extract_host(\"$path\") as bucket, regexp_extract(\"$path\", '\\w*\\/\\/([\\w-]+).([\\w\\.\\/\\=]+)\\/.*', 2) as table_location from {} limit 1".format(table_name_qualified)
        with closing(conn.cursor()) as cur:
            cur.execute(location_query)
            location_details = cur.fetchall()
            return location_details[0]      
            
    
    
    def __download_data_s3(bucket, table_location, target_location):
        logger.warning('Starting file download')
        s3_uri = "s3://{}/{}".format(bucket, table_location)
        CloudPath(s3_uri).download_to(target_location)
        logger.warning('Files downloaded to: %s', target_location)
        
    
    def download_data_from_query(user_schema, select_query, user, password, target_location, file_format='ORC', compression='GZIP'):
        conn = edputils.get_trino_connection(user, password)
        tmp_table_qualified = edputils.__create_table_from_query(user_schema, select_query, conn, file_format, compression)
        location_details = edputils.__get_table_location(tmp_table_qualified, conn)
        edputils.close_trino_connection(conn)
        edputils.__download_data_s3(location_details[0], location_details[1], target_location)
        
    
    def __read_data_location(bucket, table_location, file_format, return_arrow_dt):
        logger.warning('Reading files from S3 started')
        s3_uri = "s3://{}/{}".format(bucket, table_location)
        if file_format.lower() == 'orc':
            s3 = boto3.resource('s3')
            my_bucket = s3.Bucket(bucket)
            s3fs = fs.S3FileSystem()
            dt = []
            for object_summary in my_bucket.objects.filter(Prefix="{}/".format(table_location)):
                file=po.ORCFile(s3fs.open_input_file("{}/{}".format(bucket,object_summary.key)))
                table=file.read()
                df=pd.DataFrame(table.to_pandas())
                dt.append(df)
            
            dt = pd.concat(dt, ignore_index=True)
            
            logger.warning('Reading files from S3 completed')
            if return_arrow_dt:
                return pa.Table.from_pandas(dt)
            else:
                return dt
                
        elif file_format.lower() == 'parquet':
            data = ds.dataset(s3_uri, format=file_format).to_table()
            
        elif file_format.lower() == 'csv':
            data = ds.dataset(s3_uri, format=file_format).to_table()
            
        else:
            logger.error('The file format:%s is not supported', file_format)
        
        logger.warning('Reading files from S3 completed')
        if return_arrow_dt:
            return data
        else:
            return data.to_pandas()
    
    
    def read_data_from_query(select_query, user=None, password=None, connection=None, user_schema='edputils', return_arrow_dt=False):
        file_format='parquet'
        compression='snappy'
        conn = edputils.get_trino_connection(user, password) if connection is None else connection
        tmp_table_qualified = edputils.__create_table_from_query(user_schema, select_query, conn, file_format, compression)
        location_details = edputils.__get_table_location(tmp_table_qualified, conn)
        data = edputils.__read_data_location(location_details[0], location_details[1], file_format, return_arrow_dt)
        edputils.__drop_table(tmp_table_qualified, conn)
        if connection is None:
            edputils.close_trino_connection(conn)
        return data


    def read_data_from_table(table_name_qualified, user=None, password=None, connection=None, file_format='ORC', return_arrow_dt=False):
        conn = edputils.get_trino_connection(user, password) if connection is None else connection
        location_details = edputils.__get_table_location(table_name_qualified, conn)
        data = edputils.__read_data_location(location_details[0], location_details[1], file_format, return_arrow_dt)
        if connection is None:
            edputils.close_trino_connection(conn)
        return data
