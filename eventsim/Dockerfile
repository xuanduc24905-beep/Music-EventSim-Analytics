FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python==2.0.2 faker==19.13.0

COPY simulator.py .

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=music-events
ENV EVENTS_PER_SECOND=10
ENV NUM_USERS=1000

CMD ["python", "simulator.py"]
