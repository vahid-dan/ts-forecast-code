delete_restart <- function(site){
  files <- aws.s3::get_bucket(bucket = "restart",
                              prefix = site,
                              region = Sys.getenv("AWS_DEFAULT_REGION"),
                              use_https = as.logical(Sys.getenv("USE_HTTPS")))
  keys <- vapply(files, `[[`, "", "Key", USE.NAMES = FALSE)
  empty <- grepl("/$", keys)
  keys <- keys[!empty]
  if(length(keys > 0)){
    for(i in 1:length(keys)){
      aws.s3::delete_object(object = keys[i],
                            bucket = "restart",
                            region = Sys.getenv("AWS_DEFAULT_REGION"),
                            use_https = as.logical(Sys.getenv("USE_HTTPS")))
    }
  }
}
