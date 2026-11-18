from confluent_kafka.admin import AdminClient

ENDERECO = "localhost:9092"
cliente = AdminClient({"bootstrap.servers": ENDERECO})
#cria objeto cliente.
#cliente: ferramenta que o programa utilizará para enviar solicitações ao Kafka.
informacoes = cliente.list_topics(timeout=5)
#objeto cliente solicita informações sobre os tópicos existentes no Kafka.
#espera até 5 segundos pela resposta.
#informacoes é objeto que conterem dados sobre tópicos e outras informações do cluster.
for nome in informacoes.topics:
    print(" -", nome)
    


