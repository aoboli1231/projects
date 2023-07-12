from flask import Flask, jsonify
import couchdb
from flask import Blueprint

general_mastodon_bp = Blueprint('general_mastodon', __name__)

app = Flask(__name__)

# connecting CouchDB
couch = couchdb.Server('http://admin:1dTY1!PWM2@172.26.133.51:5984/')
db = couch['mastodon_data_v3']

# count of tweet every hour 
@general_mastodon_bp.route('/general/mastondon/count_hourly')
def get_data():
    results = db.view('_design/general/_view/hourly', group=True)
    data = {}
    for row in results:
        if row.key != None:
            data[(row.key+10)%24] = row.value

    return jsonify(data)

# count of tweet at night/day
@general_mastodon_bp.route('/general/mastondon/count_day_night')
def get_data2():
    results = db.view('_design/general/_view/hourly', group=True)
    data = {"day": 0, "night": 0}
    for row in results:
        if row.key != None:
            if (row.key+10)%24 in [0, 1, 2, 3, 4, 5, 22, 23]:
                data["night"] += row.value
            else:
                data["day"] += row.value
    return data

# language_hour
@general_mastodon_bp.route('/general/mastodon/language_hour')
def get_data3():
    results = db.view('_design/general/_view/hourly_language', group=True)

    data = []
    all_language = []
    for row in results:
        if row.key[0] == None:
            continue
        if row.key[0] not in all_language:
            all_language.append(row.key[0])
        data.append(row)

    data1 = {}
    for i in all_language:
        new = {}
        for j in data:
            if j.key[0] == i:
                new[(j.key[1]+10)%24] = j.value
        data1[i] = new.copy()
    return jsonify(data1)

# language_day_night
@general_mastodon_bp.route('/general/mastodon/language_day_night')
def get_data4():
    results = db.view('_design/general/_view/hourly_language', group=True)

    data = []
    all_language = []
    for row in results:
        if row.key[0] == None:
            continue
        if row.key[0] not in all_language:
            all_language.append(row.key[0])
        data.append(row)

    data1 = {}
    for i in all_language:
        new = {"day":0, "night":0}
        for j in data:
            if j.key[0] == i:
                if (j.key[1]+10)%24 in [0, 1, 2, 3, 4, 5, 22, 23]:
                    new["night"] += j.value
                else:
                    new["day"] += j.value
        data1[i] = new.copy()
    return jsonify(data1)
