# trade-website-monitoring

Small project for a friend in the import industry trying to keep up with updates to government trade list website in 2019/2020 and limit her need to manually scroll through the website daily. Examples of the airflow dag and input notebook included. Daily pipeline parses the website using BeautifulSoup, identifies dates/text, and sends a daily email whether there is an update to the website.
