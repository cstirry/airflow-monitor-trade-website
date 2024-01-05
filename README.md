# airflow-trade-website-monitoring

Pipeline to monitor government trade list website actively being updated (in 2019/2020) to prevent friend in import industry to have to manually scroll through the website daily.

An airflow dag is set up to run daily, includes 3 operators. It executes a Jupyer notebook that uses a parameter set by [papermill](https://papermill.readthedocs.io/en/latest/). Parses the trade website using [BeautifulSoup](https://pypi.org/project/beautifulsoup4/) and identifies dates and related text. Sends a daily email with any website updates attaching a PDF created with [nbconvert](https://nbconvert.readthedocs.io/en/latest/).
