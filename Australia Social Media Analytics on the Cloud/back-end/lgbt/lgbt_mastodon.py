from flask import Flask, jsonify
import couchdb
from flask import Blueprint

lgbt_mastodon_bp = Blueprint('lgbt_mastodon', __name__)

app = Flask(__name__)
# connecting CouchDB
couch = couchdb.Server('http://admin:1dTY1!PWM2@172.26.133.51:5984/')
db = couch['mastodon_data_v3']

# overall_sentiment
@lgbt_mastodon_bp.route('/lgbt/mastondon/overall_sentiment')
def get_data0():
    results = db.view('_design/lgbt_sentiment/_view/lgbt_overall', group=True)
    data = {}
    for row in results:
        data[row.key[0]] = row.value
    return jsonify(data)

# sentiment_lang
@lgbt_mastodon_bp.route('/lgbt/mastondon/sentiment_lang')
def get_data():
    results = db.view('_design/lgbt_sentiment/_view/language_sentiment', group=True)
    
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
        neg = 0
        pos = 0
        neu = 0
        total = 0
        mag = 0
        sen_num = 0
        for j in data:
            if j.key[0] == i:
                total += j.value["count"]
                mag += j.value["average_magnitude"]
                sen_num += 1
                if j.key[1] == "positive":
                    pos += j.value["count"]
                if j.key[1] == "negative":
                    neg += j.value["count"]
                if j.key[1] == "neutral":
                    neu += j.value["count"]
        new_item = {
            "total": total,
            "neg%": neg/total,
            "pos%": pos/total,
            "neu%": neu/total,
            "ave_mag": mag/sen_num
        }
        data1[i] = new_item
        
    return jsonify(data1)