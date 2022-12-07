#' @title Read data from EDP using Presto/Trino SQL
#'
#' @description Function to read data from EDP with SQL using Trino and Apache Arrow
#'
#' @param select_query Select query to read the data
#'
#' @param user Trino user id
#'
#' @param password Trino password
#'
#' @param host Optional Trino host
#'
#' @param port Optional Trino port
#'
#' @param connection Optional Trino connection. If not provided Trino user and password is required
#'
#' @param user_schema User schema used to create temporary table. Default is edputils
#'
#' @return data.table
#'
#' @examples read_data_from_query(user_schema='tmp', select_query='select * from tmp.abc', connection)
#'
#' @export
read_data_from_query <- function(select_query,user,password,host='https://enterprisehadoopuser.seagate.com',port=8081,connection,user_schema='edputils') {

    if (missing(connection)) {
        connection <- get_trino_connection(user,password,host,port)
    }

    RPresto::dbFetch(RPresto::dbSendQuery(connection,"set session hive.compression_codec='SNAPPY'"), -1)
    RPresto::dbFetch(RPresto::dbSendQuery(connection,"set session hive.collect_column_statistics_on_write=false"), -1)

    tmp_table = paste('edputils', user_schema, sample(100000000000:999999999999,1), sep="_")
    tmp_table_qualified = paste(user_schema, tmp_table, sep='.')
    create_query = paste0("create table ", tmp_table_qualified, " with (format = 'parquet') as ", select_query)
    drop_query = paste0("drop table if exists ", tmp_table_qualified)

    print(paste("Query Started at:", Sys.time()))
    RPresto::dbFetch(RPresto::dbSendQuery(connection, create_query), -1)
    print(paste("Query Completed at:", Sys.time()))

    # Getting user bucket and table location
    location_query = paste("select url_extract_host(\"$path\") as bucket, regexp_extract(\"$path\", '\\w*\\/\\/([\\w-]+).([\\w\\.\\/\\=]+)\\/.*', 2) as table_location from", tmp_table_qualified, "limit 1")
    location_details = RPresto::dbFetch(RPresto::dbSendQuery(connection, location_query), -1)
    user_bucket = location_details[1,1]
    table_location = location_details[1,2]
    print(paste("Derived Bucket:", user_bucket))
    print(paste("Derived Location:", table_location))

    print(paste("Reading data from S3 started at:", Sys.time()))
    data <- read_data_from_location(user_bucket, table_location)
    print(paste("Reading completed at:", Sys.time()))

    RPresto::dbFetch(RPresto::dbSendQuery(connection, drop_query), -1)

    if (missing(user) == FALSE) {
        disconnect_trino_connection(connection)
    }
    return(data)
}

