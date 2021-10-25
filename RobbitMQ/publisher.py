# example_publisher.py
import pika, os, logging, sys
logging.basicConfig()

# Parse CLODUAMQP_URL (fallback to localhost)
url = os.environ.get('CLOUDAMQP_URL', 'amqps://hxbmajdt:RDTTIox2Rmes-fCamk6DxDFv7HF2tLNK@jackal.rmq.cloudamqp.com/hxbmajdt')
params = pika.URLParameters(url)
params.socket_timeout = 5

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='pdfprocess_aula')

# send a message
channel.basic_publish(exchange='Diogo', routing_key='pdfprocess_aula', body = sys.argv[1])
print ("[x] Message sent to consumer")

connection.close()