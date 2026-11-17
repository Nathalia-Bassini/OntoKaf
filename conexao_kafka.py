from confluent_kafka.admin import AdminClient

admin = AdminClient({"bootstrap.servers": "localhost:9092"})
info = admin.list_topics(timeout=5)
print("CONECTOU! Tópicos existentes:", list(info.topics))

