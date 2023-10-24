import pika, sys, os, time
from pymongo import MongoClient
import gridfs
from convert import to_mp3

def main():
    client = MongoClient('mongodb://localhost', 27017)
    db_videos = client.videos
    db_mp3s = client.mp3s
    # gridfs
    fs_videos = gridfs.GridFS(db_videos)
    fs_mp3s = gridfs.GridFS(db_mp3s)

    # rabbit / host to be changed if run in the cluster
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    def callback(channel, method, properties, body):
        err = to_mp3.start(body, fs_videos, fs_mp3s, channel)
        if err:
            channel.basic_nack(delivery_tag=method.delivery_tag)
        else:
            channel.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume('video', on_message_callback=callback)

    print('Waiting for messages. To exit press CTRL + C ...')

    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)