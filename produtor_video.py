"""
produtor_video.py - envia imagens para o Kafka, para você testar sem câmeras IP.

Uso:
    python produtor_video.py              # usa a webcam do computador
    python produtor_video.py video.mp4    # usa um arquivo de vídeo
"""
import sys
import time

import cv2
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPICO = "meu-topico-de-video"
CAMERA_ID = "cam_teste"

fonte = sys.argv[1] if len(sys.argv) > 1 else "0"
if fonte.isdigit():
    fonte = int(fonte)               # webcam é um número (0 = a primeira)

captura = cv2.VideoCapture(fonte)
if not captura.isOpened():
    sys.exit(f"Não consegui abrir a fonte de vídeo: {fonte}")

producer = Producer({"bootstrap.servers": BOOTSTRAP})
print(f"Enviando '{fonte}' como '{CAMERA_ID}' para '{TOPICO}'. Ctrl+C para parar.")

try:
    while True:
        ok, frame = captura.read()
        if not ok:
            # Se for arquivo de vídeo e acabou, volta para o começo.
            captura.set(cv2.CAP_PROP_POS_FRAMES, 0)
            ok, frame = captura.read()
            if not ok:
                break

        frame = cv2.resize(frame, (640, 480))
        ok, jpeg = cv2.imencode(".jpg", frame)      # imagem -> bytes JPEG
        if ok:
            try:
                producer.produce(
                    TOPICO,
                    key=CAMERA_ID.encode("utf-8"),
                    value=jpeg.tobytes(),
                )
            except BufferError:
                producer.poll(0.1)                  # fila cheia: espera esvaziar
            producer.poll(0)

        time.sleep(1 / 30)                          # ~30 imagens por segundo
except KeyboardInterrupt:
    print("\nEncerrando...")
finally:
    captura.release()
    producer.flush()