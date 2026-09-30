import gzip
import io
import sys
sys.path.append('./kafka/')
from producer import KafkaProducers
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
producer=KafkaProducers()

def read_gzip_file(file_path):
    file_path=file_path.content
    with gzip.GzipFile(fileobj=io.BytesIO(file_path), mode='rb') as data:
        content=data.read().decode('utf-8').splitlines()
    for line in content:
        logger.info("Data is successfully being pushed to kafka.")
        producer.produce_data(bytes(line, encoding='utf8'))
    return file_path.status_code