import pika, json, tempfile, os
from bson.objectid import ObjectId
import moviepy.editor


def start(message, fs_videos, fs_mp3s, channel):
    message = json.loads(message)
    # empty temp file
    tf = tempfile.NamedTemporaryFile()
    # video contents
    out = fs_videos.get(ObjectId(message['video_file_id']))
    # add video contents to empty file
    tf.write(out.read())
    # create audio from video temp file
    audio = moviepy.editor.VideoFileClip(tf.name).audio
    tf.close()

    # write audio to the file
    tf_path = tempfile.gettempdir() + f'/{message["video_file_id"]}.mp3'
    audio.write_audiofile(tf_path)

    # save file to mongo db
    with open(tf_path, 'rb') as file:
        data = file.read()
        fid = fs_mp3s.put(data)
    os.remove(tf_path)

    # update message
    message['mp3_file_id'] = str(fid)

    try:
        channel.basic_publish(
            exchange='',
            routing_key='mp3',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                )
            )
    except Exception as err:
        fs_mp3s.delete(fid)
        return "Failed to publish message"