package spark

/**
  *
  * @param project_name name of the project
  * @param page_title title of the page
  * @param num_requests number of requests
  * @param content_size size of the content
  */
case class Page(
                   project_name: String,
                   page_title: String,
                   num_requests: Long,
                   content_size: Long)
