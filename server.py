import datetime
import requests

from flask import Flask, request, json, Response
from flask_restful import Resource, Api
from flask_jsonpify import jsonify
from flask_mysqldb import MySQL

from sqlalchemy import create_engine
from json import dumps

from moviepy.editor import VideoFileClip

db_connect = create_engine('sqlite:///chinook.db')
app = Flask(__name__)
mysql = MySQL(app)
api = Api(app)

# MySQL configurations
app.config['MYSQL_USER'] = ''
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'paperclipsa'
app.config['MYSQL_HOST'] = '127.0.0.1'
app.config['MYSQL_PORT'] = 3306

# Get Data Routes

class Streams(Resource):
    def get(self):
        cur = mysql.connection.cursor() # connect to database
        cur.execute('''SELECT wow_app_name FROM venues WHERE venue_type="indoor_soccer" AND active_status="active"''')
        rv = cur.fetchall()
        return jsonify(rv)
        
api.add_resource(Streams, '/streams')

# Post Routes

@app.route('/highlights', methods = ['POST'])
def api_highlights():

    start_path = 'C:/wowza/content'
    clip_file_name = request.json['file_name']
    clip_end_time = request.json['start_time']
    clip_highlight_name = request.json['highlight_name']

    # Get time in seconds of clip start time

    ftr = [3600,60,1]
    end_time_seconds = sum([a*b for a,b in zip(ftr, map(int,clip_end_time.split(':')))]) - 20 # Subtract 20 seconds for duration of highlight
    
    # Convert to time delta to get start time as a string

    clip_start_time = datetime.timedelta(seconds=end_time_seconds).__str__()

    clip = VideoFileClip(start_path +'/' + clip_file_name + ".mp4")
    newclip = clip.subclip(clip_start_time, clip_end_time)
    newclip.write_videofile("C:/wowza/highlights/"+clip_highlight_name+".mp4")    

    return json.dumps(request.json)

@app.route('/check-streams-over-limit')
def check_streams_over_limit():
    # List Venue App Names from indoor soccer venues
    venues = {}
    incomming_streams_reached_limit = []

    cur = mysql.connection.cursor() # connect to database
    cur.execute('''SELECT wow_app_name, id FROM venues WHERE venue_type="indoor_soccer" AND active_status="active"''')
    app_names = cur.fetchall()

    cur = mysql.connection.cursor()
    cur.execute('''SELECT name FROM streams WHERE stream_type="live"''')
    streams = cur.fetchall()

    for app_name in app_names:
        venues[app_name[0]] = {"wow_app_name": app_name[0], "id": app_name[1], "incoming_streams": {}, "live_streams": {} }

    for key, value in venues.items():
        
        incoming_streams = requests.get('http://127.0.0.1:8087/v2/servers/_defaultServer_/vhosts/_defaultVHost_/applications/'+ value['wow_app_name'] + '/instances/_definst_', headers={'Accept': 'application/json'})
            
        if 'incomingStreams' in json.loads(incoming_streams.text):
            value['incoming_streams'] = json.loads(incoming_streams.text)['incomingStreams']

        cur = mysql.connection.cursor()
        venue_id = value['id']
        cur.execute("SELECT name FROM streams WHERE venue_id='"+str(venue_id)+"' AND stream_type='live'")
        venue_live_streams = cur.fetchall()
        value['live_streams'] = venue_live_streams

    # Delete stream file, update database record to change from live to vod.

    for key, value in venues.items():
        if(value['live_streams']):

            for stream in value['live_streams']:

                for active_stream in value['incoming_streams']:

                    if (active_stream['name'] == stream[0] + '.stream_source'):

                        if(active_stream['isConnected']):
                            response = requests.get('http://127.0.0.1:8087/v2/servers/_defaultServer_/vhosts/_defaultVHost_/applications/'+value['wow_app_name']+'/instances/_definst_/incomingstreams/'+active_stream['name']+'/monitoring/current', headers={'Accept': 'application/json'})
                            active_live_stream = json.loads(response.text)
                            if(active_live_stream['uptime'] >=20): # 4500 hour and 15 minutes  hour 3600

                                if(active_stream['isRecordingSet']):
                                    stop_rec_response = requests.put('http://127.0.0.1:8087/v2/servers/_defaultServer_/vhosts/_defaultVHost_/applications/'+value['wow_app_name']+'/instances/_definst_/streamrecorders/'+active_stream['name']+'/actions/stopRecording', headers={'Accept': 'application/json'})
                                    print(json.loads(stop_rec_response.text))

                                    if(json.loads(stop_rec_response.text)["success"]):
                                        active_stream_response = requests.put('http://127.0.0.1:8087/v2/servers/_defaultServer_/vhosts/_defaultVHost_/applications/'+value['wow_app_name']+'/instances/_definst_/incomingstreams/'+stream[0] + '.stream'+'/actions/disconnectStream', headers={'Accept': 'application/json'})
                                        print(json.loads(active_stream_response.text))

                                        if(json.loads(active_stream_response.text)["success"]):
                                            delete_streamfile_response = requests.delete('http://127.0.0.1:8087/v2/servers/_defaultServer_/vhosts/_defaultVHost_/applications/'+value['wow_app_name']+'/streamfiles/' + stream[0], headers={'Accept': 'application/json'})
                                            
                                            if(json.loads(delete_streamfile_response.text)["success"]):
                                                print("success: stream file->(" + stream[0] + ") deleted")
                                                conn = mysql.connect
                                                cur = conn.cursor() # connect to database
                                                cur.execute("UPDATE streams SET stream_type='vod' WHERE name='{0}'".format(stream[0]))
                                                conn.commit()

                                            else:
                                                print("fail: stream file->(" + stream[0] +") could not be deleted")

                                        else:
                                            print("could not disconnect: " + stream[0])
                                            
                                    else:
                                        print("could not stop recording: " + stream[0])
                                        print(json.loads(stop_rec_response.text))

                                else:
                                    active_stream_response = requests.get('http://127.0.0.1:8087/v2/servers/_defaultServer_/vhosts/_defaultVHost_/applications/'+value['wow_app_name']+'/instances/_definst_/incomingstreams/'+stream[0] + '.stream'+'/actions/disconnectStream', headers={'Accept': 'application/json'})
                                    
                                    if(json.loads(active_stream_response.text)["success"]):
                                        print("success: " + stream[0])
                                    else:
                                        print("could not disconnect stream: " + stream[0])

                            else:                
                                print('not over time: ' + stream[0])
                                print(stream[0] + ": uptime-> ("+ str(active_live_stream['uptime']) + ")")
                           


    #print(venues)

    response = requests.get('http://127.0.0.1:8087/v2/servers/_defaultServer_/vhosts/_defaultVHost_/applications/Fast_Sport_Fusion_Old_Parks/instances/_definst_/incomingstreams/Test_Player_3_VS_Test_Player_4_2018_07_9_15-16-43.stream_source/monitoring/current', headers={'Accept': 'application/json'})
    dict_streams = json.loads(response.text)
    #print(dict_streams)
    
    return jsonify(venues), 200, {'ContentType': 'application/json'}

if __name__ == '__main__':
     app.run(port='5002')