# chat with rabitmq
import threading, time 
import pika, os, logging, sys
import json
import datetime
import logging




def publisher(meu_nome, nome_usuario, pub_exchange, tipo_destinatario):
    print("Iniciando thread publisher " + str(meu_nome))
    logging.basicConfig()

    # Parse CLOUDAMQP_URL (fallback to RabbitMQ broker / localhost / cloudamqp)
    url = os.environ.get('CLOUDAMQP_URL','amqps://tqzvyxix:oGh4wcsnvBrG2U5zzQBDgkcFH_ff8B8C@jackal.rmq.cloudamqp.com/tqzvyxix')
    params = pika.URLParameters(url)
    params.socket_timeout = 5   # Altera o timeout de 0.25s para 5s evitar problemas de conexoes lentas

    # Estabelecendo a conexao, o canal e a fila
    connection = pika.BlockingConnection(params)

    # inicia um canal
    channel = connection.channel()

    # queueueu
    channel.queue_declare(queue=nome_usuario)

    # declara exchange
    channel.exchange_declare(exchange=pub_exchange, exchange_type="fanout")


    # entrada console
    print("Digite sua mensagem:")
    msg = input()

    mensagem = {
         "user": meu_nome,
         "timestamp": str(datetime.datetime.now().replace(microsecond=0).isoformat()),
         "message": msg,
         "source": tipo_destinatario
         }

    # Enviando uma mensagem (exchange=default, ...., ...)
    if tipo_destinatario == "user":
        channel.basic_publish(exchange='', routing_key=nome_usuario, body=json.dumps(mensagem))
    else:
        channel.basic_publish(exchange="sisdis", routing_key='', body=json.dumps(mensagem))


    print("Mensagem enviada para o consumidor[X]")


    connection.close()


def msg_process_function(msg):

    print(" [x] Mensagem recebida: " + str(msg))
    time.sleep(0.5) # Atraso de 5 segundos

    logging.basicConfig(filename="logSaida.log",
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        level=logging.INFO)

    logger = logging.getLogger("root")
    logger.info(msg)

    return


# Função a ser chamada quando uma mensagem chega
def processa_msg(ch,method,properties,body):
    msg_process_function(body)
    

def consumer(pub_queue1, pub_exchange):
    print("Iniciando thread consumer " + str(pub_queue1))
    
    # Access the CLOUDAMQP_URL environment variable and parse it (fallback to RabbitMQ broker / localhost / cloudamqp
    url = os.environ.get('CLOUDAMQP_URL','amqps://tqzvyxix:oGh4wcsnvBrG2U5zzQBDgkcFH_ff8B8C@jackal.rmq.cloudamqp.com/tqzvyxix')
    params = pika.URLParameters(url)

    # cria conexao bloqueante
    connection = pika.BlockingConnection(params)

    # Inicia um canal
    channel = connection.channel()

    # declara fila
    channel.queue_declare(queue=pub_queue1)

    # declara exchange
    channel.exchange_declare(exchange=pub_exchange, exchange_type="fanout")

    # binda fila com exchange
    channel.queue_bind(queue=pub_queue1, exchange=pub_exchange)

    # Configura a assinatura (subscribe) na fila
    channel.basic_consume(pub_queue1, processa_msg, auto_ack=True)

    # Inicia o consumo (blocos)
    channel.start_consuming()

    connection.close()


if __name__ == "__main__":
    
    print("Digite seu nome de usuário")
    meu_nome = input()

    while(1):
        
        # entrada console
        print("Para mandar mensagem individual, digite 1, para mandar no grupo digite 2, para abrir um consumer digite 3")

        opcao = input()

        if opcao == '1':
            print("Digite o nome do usuário:")
            nome_usuario = input()
            t_publisher = threading.Thread(target=publisher, args=(meu_nome, nome_usuario, nome_usuario,  "user"))
            t_publisher.start()
            t_publisher.join()


        elif opcao == '2':
            t_publisher = threading.Thread(target=publisher, args=(meu_nome, meu_nome,  "sisdis", "group",))
            t_publisher.start()
            t_publisher.join()
        
        elif opcao == '3':
            t_consumer = threading.Thread(target=consumer, args=(meu_nome, "sisdis",))
            t_consumer.start()
        

    while(1):
        time.sleep(1000)