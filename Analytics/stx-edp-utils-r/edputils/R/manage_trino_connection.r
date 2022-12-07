#' @title Get Trino Connection for EDP Trino(formally EHC Presto)
#'
#' @description Function to get Trino Connection for EDP Trino(formally EHC Presto)
#'
#' @param user Trino user id
#'
#' @param password Trino password
#'
#' @param host Optional Trino host
#'
#' @param port Optional Trino port
#'
#' @return connection
#'
#' @examples get_trino_connection(user='uuuuu', password='ppppp')
#'
#' @export
get_trino_connection <- function(user, password, host='https://enterprisehadoopuser.seagate.com', port=8081) {

  httr::reset_config()
  Sys.setenv(https_proxy='')
  httr::set_config(
    httr::config(
      httpauth=1,
      userpwd=paste(user, password, sep=':'),
      ssl_verifypeer=0L
    )
  )

  connection <- DBI::dbConnect(RPresto::Presto(), host=host, schema='default', catalog='hive', user=user, port=port)
  return(connection)
}


#' @title Get Trino Connection for EDP Trino(formally EHC Presto)
#'
#' @description Function to get Trino Connection for EDP Trino(formally EHC Presto)
#'
#' @param connection
#'
#' @return None
#'
#' @examples disconnect_trino_connection(connection)
#'
#' @export
disconnect_trino_connection <- function(connection) {

  RPresto::dbDisconnect(connection)
  print('Connection disconnected')
}
