#' @title Read data from EDP table
#'
#' @description Function to read data from EDP table location using Apache Arrow
#'
#' @param user_bucket s3 bucket name where data is stored
#'
#' @param table_location s3 prefix of the table location
#'
#' @return data.table
#'
#' @examples read_data_from_table('bucket-name', '/location/to/table')
#'
#' @export
read_data_from_location <- function(user_bucket,table_location) {

    file_details = as.data.frame(aws.s3::get_bucket(bucket=user_bucket, prefix=table_location))
    file_size = round(sum(as.numeric(file_details$Size))/1024/1024, digits=2)
    print(paste("Number of Files to Download:", nrow(file_details), ", Total Size:", file_size, "MB"))
    files = file_details$Key

    dataset = lapply(files, function(x) {aws.s3::s3read_using(FUN = arrow::read_parquet, object = x, bucket = user_bucket)})
    data = do.call(rbind, dataset)
    return(data)
}
