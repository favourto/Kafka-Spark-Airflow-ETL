import requests
from datetime import datetime , timedelta
import re
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WikimediaUrl():

    def __init__(self):
        self.base_url = "https://dumps.wikimedia.org/other/clickstream"
        self.current_date = datetime.now()

    def get_url(self):
        self.formatted_date = self.current_date.strftime("%Y-%m")
        while True:
            url = f"{self.base_url}/{self.formatted_date}"
            response = requests.get(url)
            if response.status_code == 200:
                logger.info("status code: 200")
                return response.text
            elif response.status_code == 404:
                # Subtract one month from the current date
                self.current_date -= timedelta(days=self.current_date.day)
                self.current_date=self.current_date.strftime("%Y-%m")
                response = requests.get(self.base_url +'/'+ self.current_date)
            else:
                raise Exception(f"Failed to fetch data from URL: {url}")
            return response.text

    def fetch_filenames(self, data):
        file_details = re.findall(r'<a href="[^"]+\.tsv\.gz">(.*?)</a>\s+(\d+-[A-Za-z]+-\d{4} \d{2}:\d{2})\s+(\d+)', data)
        result = [
            [
                self.base_url.split('//')[1] + "/" + re.search(r'(\d{4}-\d{2})', file_name.strip()).group(1) + "/" + file_name.strip(),
                date.strip(), 
                int(size.strip())
            ] 
            for file_name, date, size in file_details
            ]
        return result
