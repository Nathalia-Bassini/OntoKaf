from confluent_kafka import Consumer

consumidor = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "aprendendo",
    "auto.offset.reset": "latest",
})
consumidor.subscribe(["meu-topico-de-video"])

recebidas = 0
while recebidas:                      # para sozinho depois de 5
    msg = consumidor.poll(1.0)            # espera até 1 segundo
    if msg is None:
        print("esperando mensagem...")
        continue
    if msg.error():
        print("erro:", msg.error())
        continue
    print("Veio da câmera:", msg.key().decode("utf-8"))


consumidor.close()