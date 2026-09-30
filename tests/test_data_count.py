import unittest
from datetime import datetime
from wikimedia.base import WikimediaUrl

class VerifyDataExist(unittest.TestCase):

    def setUp(self):
        self.wiki = WikimediaUrl()
        self.get_url = self.wiki.get_url()
        self.today = datetime.today()
        self.month_abbr = self.today.strftime('%b')

    def test_verify_url_get_data_from(self):
        no_of_urls = len(self.wiki.fetch_filenames(self.get_url))
        self.assertEqual(no_of_urls,11)
    
    def test_verify_month_data_to_pull_from(self):
        data_month = self.wiki.fetch_filenames(self.get_url)
        data_month = data_month[0][1].split('-')[1]
        self.assertEqual(data_month,self.month_abbr)

if __name__ == "__main__":
    unittest.main(argv=[""], verbosity=2, exit=True)