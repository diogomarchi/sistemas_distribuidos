# chat with rabitmq
import threading, time 
import pika, os, logging, sys


pub_routing_key='Chat'
pub_exchange='Diogo'


def publisher (pub_queue1):
    print("Iniciando thread publisher" + str(pub_queue1))
    
    logging.basicConfig()

    # Parse CLOUDAMQP_URL (fallback to RabbitMQ broker / localhost / cloudamqp)
    url = os.environ.get('CLOUDAMQP_URL','amqps://hxbmajdt:RDTTIox2Rmes-fCamk6DxDFv7HF2tLNK@jackal.rmq.cloudamqp.com/hxbmajdt')
    params = pika.URLParameters(url)
    params.socket_timeout = 5 # Altera o timeout de 0.25s para 5s evitar problemas de conexoes lentas

    # Estabelecendo a conexao, o canal e a fila
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=pub_queue1)

    
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
    

def consumer (pub_queue1):
    print("Iniciando thread consumer" + str(pub_queue1))
    
    # Access the CLOUDAMQP_URL environment variable and parse it (fallback to RabbitMQ broker / localhost / cloudamqp
    url = os.environ.get('CLOUDAMQP_URL','amqps://hxbmajdt:RDTTIox2Rmes-fCamk6DxDFv7HF2tLNK@jackal.rmq.cloudamqp.com/hxbmajdt')
    params = pika.URLParameters(url)

    connection = pika.BlockingConnection(params)
    channel = connection.channel() # Inicia um canal
    channel.queue_declare(queue=pub_queue1)

    # Configura a assinatura (subscribe) na fila
    channel.basic_consume(pub_queue1, processa_msg, auto_ack=True)

    # Inicia o consumo (blocos)
    channel.start_consuming()

    connection.close()
 
def iniciaGrupo():
    t_consumer = threading.Thread(target=consumer, args=('George', ))
    t_consumer.start()    
    
    t_publisher = threading.Thread(target=publisher, args=('George', ))
    t_publisher.start()   


    t_consumer = threading.Thread(target=consumer, args=('teddy', ))
    t_consumer.start()    
    
    t_publisher = threading.Thread(target=publisher, args=('teddy', ))
    t_publisher.start()   


def iniciaIndividual(Fila):
    t_consumer = threading.Thread(target=consumer, args=(Fila, ))
    t_consumer.start()    
    
    t_publisher = threading.Thread(target=publisher, args=(Fila, ))
    t_publisher.start()  

# entrada console
print("Para mandar mensagem individual, digite 1, para mandar no grupo digite 2")
opcao = input()  

if opcao == '1':
  print("Digite o nome da fila")
  opcao2 = input()
else:
  opcao2 = 'Grupo'

if opcao2 == 'Grupo':
  iniciaGrupo()
else:
  iniciaIndividual(opcao2)


while(1):
    time.sleep(1000)

