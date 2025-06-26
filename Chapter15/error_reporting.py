import google.cloud.logging  
from google.cloud.logging.handlers import CloudLoggingHandler  
import logging   

client = google.cloud.logging.Client()  
handler = CloudLoggingHandler(client)  
logger = logging.getLogger('datapipeline-errors')  
logger.setLevel(logging.ERROR)  
logger.addHandler(handler)  
  
try:  
    # Some transformation logic  
    raise ValueError("Invalid record format")  
except Exception as e:  
    logger.exception("Pipeline error: %s", e) 
    