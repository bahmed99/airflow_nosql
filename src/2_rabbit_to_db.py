import pika
from pymongo import MongoClient
import json
import time

client = MongoClient('mongodb://mongo:mongo@mongoDB:27017/')
db = client['airflow'] 
collection = db['posts'] 

def safe_connect_rabbitmq():
    channel = None
    while not channel:
        try:
            connection = pika.BlockingConnection(pika.URLParameters("amqp://rabbitmq"))
            channel = connection.channel()
        except pika.exceptions.AMQPConnectionError:
            time.sleep(1)
    return channel

def post_exists(post_id):
    return collection.find_one({"@Id": post_id}) is not None

def callback(ch, method, properties, body):
    data_string = body.decode("utf-8")
    data = json.loads(data_string)

    post_id = data["@Id"]

    if not post_exists(post_id):
        collection.insert_one(data)
        print(f"Inserted post with ID: {post_id}")
    else:
        print(f"Post with ID: {post_id} already exists. Ignored.")

    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    print("Starting rabbit_to_mongodb.py")

    channel = safe_connect_rabbitmq()

    channel.queue_declare(queue='posts_to_db')
    channel.basic_consume(
        queue='posts_to_db',
        on_message_callback=callback
    )
    channel.start_consuming()
    print("Done")

if __name__ == "__main__":
    main()
