from kafka import KafkaProducer
from datetime import datetime 
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class KafkaProducers():

    def __init__(self):
        self.topic_name : str = "clickstream_data"
        self.server : str = 'localhost:9092'
        self.current_date = datetime.now()

    def produce_data(self, data):
        producer = KafkaProducer(
                            bootstrap_servers=[self.server],
                            api_version= (2,0,2))
        try:               
            producer.send(
                self.topic_name, 
                value=data
                )
            producer.flush()
            producer.close()
        except Exception as e:
            logger.error(f"Exception occurred: {e}")            

