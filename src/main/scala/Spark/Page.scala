package Spark

/**
  *
  * @param project_name
  * @param page_title
  * @param num_requests
  * @param content_size
  */
case class Page(
                   project_name: String,
                   page_title: String,
                   num_requests: Long,
                   content_size: Long)
