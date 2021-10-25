# chat with rabitmq
import threading, time 
import pika, os, logging, sys

pub_queue = 'FilaMensagem'
pub_routing_key='chat'
pub_exchange='Chat'

con_queue = 'FilaMensagem'

def publisher ():
    print("Iniciando thread publisher")
    
    logging.basicConfig()

    # Parse CLOUDAMQP_URL (fallback to RabbitMQ broker / localhost / cloudamqp)
    url = os.environ.get('CLOUDAMQP_URL','amqps://mswtmfbs:H3USewUvn1YBaWJtwdJp5tumRMsM_-Zk@jackal.rmq.cloudamqp.com/mswtmfbs')
    params = pika.URLParameters(url)
    params.socket_timeout = 5 # Altera o timeout de 0.25s para 5s evitar problemas de conexoes lentas

    # Estabelecendo a conexao, o canal e a fila
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=pub_queue)

    
    while (1) :
        # entrada console
        print("Digite sua mensagem:")
        mensagem = input()
        
        # Enviando uma mensagem (exchange=default, ....,...)
        # Envio para o exchange 'professor'
        channel.basic_publish(exchange=pub_exchange, routing_key=pub_routing_key, body=mensagem)
        print("[x] Mensagem enviada para o consumidor")

    connection.close()



def msg_process_function(msg):    
    print(" [x] Mensagem recebida: "+ str(msg))
    time.sleep(0.5) # Atraso de 5 segundos
    return;
    
# Função a ser chamada quando uma mensagem chega
def processa_msg(ch,method,properties,body):
    msg_process_function(body)
    

def consumer ():
    print("Iniciando thread consumer")
    
    # Access the CLOUDAMQP_URL environment variable and parse it (fallback to RabbitMQ broker / localhost / cloudamqp
    url = os.environ.get('CLOUDAMQP_URL','amqps://mswtmfbs:H3USewUvn1YBaWJtwdJp5tumRMsM_-Zk@jackal.rmq.cloudamqp.com/mswtmfbs')
    params = pika.URLParameters(url)

    connection = pika.BlockingConnection(params)
    channel = connection.channel() # Inicia um canal
    channel.queue_declare(queue=con_queue)

    # Configura a assinatura (subscribe) na fila
    channel.basic_consume(con_queue, processa_msg, auto_ack=True)

    # Inicia o consumo (blocos)
    channel.start_consuming()

    connection.close()
    
t_consumer = threading.Thread(target=consumer, args=())
t_consumer.start()    
    
t_publisher = threading.Thread(target=publisher, args=())
t_publisher.start()    


while(1):
    time.sleep(1000)

