import json
from flask import Flask, jsonify
import couchdb
from flask import Blueprint
import re

war_twitter_bp = Blueprint('war_twitter', __name__)

app = Flask(__name__)

couch = couchdb.Server('http://admin:1dTY1!PWM2@172.26.133.51:5984/')
db = couch['huge-twitter-v2']
project_prefix='/home/ubuntu/ccc'
with open(f'{project_prefix}/Data/geoJSON_data/state.json', 'r') as f:
        sudo_state_geo = json.load(f)
with open(f'{project_prefix}/Data/geoJSON_data/postcode.json', 'r') as f1:
        sudo_postcode_geo = json.load(f1)    
with open(f'{project_prefix}/Data/geoJSON_data/suburb.json', 'r') as f2:
        sudo_suburb_geo = json.load(f2)    

# total sentiment
@war_twitter_bp.route('/war/twitter/total_sentiment')
def get_points6():
    results = db.view('_design/sentiment/_view/RUwar', group=True)
    data = {}
    for row in results:
        data[row.key] = row.value

    return jsonify(data)

# state sentiment
@war_twitter_bp.route('/war/twitter/state_sentiment')
def get_points():
    results = db.view('_design/sentiment/_view/sentiment_state', group=True)

    data = []
    all_state = []
    for row in results:
        if row.key[0] == None:
            continue
        if row.key[0] not in all_state:
            all_state.append(row.key[0])
        data.append(row)

    data1 = {}
    for i in all_state:
        neg = 0
        pos = 0
        neu = 0
        total = 0
        mag = 0
        for j in data:
            if j.key[0] == i:
                total += j.value["count"]
                mag += j.value["average_magnitude"]
                if j.key[1] == "positive":
                    pos += j.value["count"]
                if j.key[1] == "negative":
                    neg += j.value["count"]
                if j.key[1] == "neutral":
                    neu += j.value["count"]

        new_item = {
            "war_total": total,
            "war_ave_mag": mag/3,
            "war_neg%": neg/total,
            "war_pos%": pos/total,
            "war_neu%": neu/total
        }
        data1[i] = new_item
    new_item1 = {
            "war_total": None,
            "war_ave_mag": None,
            "war_neg%": None,
            "war_pos%": None,
            "war_neu%": None
        }
    
    for feature in sudo_state_geo['features']:
        if feature["properties"]["STE_NAME21"].lower() not in all_state:
            for j in new_item1:
                feature["properties"][j] = new_item1[j]
        for row in data1:
            if feature["properties"]["STE_NAME21"].lower() == row:
                
                for i in data1[row]:
                    feature["properties"][i] = data1[row][i]


    with open(f'{project_prefix}/back-end/geojson_output/war_state.json', 'w') as file:
        json.dump(sudo_state_geo, file)
    with open(f'{project_prefix}/back-end/geojson_output/war_state.json', 'r') as f:
        geo = json.load(f)
    return jsonify(geo)

# monthly_state_sentiment
@war_twitter_bp.route('/war/twitter/monthly_state_sentiment')
def get_points5():
    results = db.view('_design/sentiment/_view/war_monthly_state_proportion', group=True)

    data = []
    all_state = []
    for row in results:
        if row.key[1] == None:
            continue
        if row.key[1] not in all_state:
            all_state.append(row.key[1])
        data.append(row)

    data1 = {}
    data2 = {}
    for i in all_state:
        all_month = []
        for j in data:
            if j.key[1] ==i:
                if j.key[0] not in all_month:
                    all_month.append(j.key[0])
        
        for month in all_month:
            neg = 0
            pos = 0
            neu = 0
            total = 0
            sen = 0
            mag = 0
            for j in data:
                if j.key[0] == month and j.key[1] == i:
                    total += j.value["count"]
                    sen += j.value["sentiment"]
                    mag += j.value["average_magnitude"]
                    if j.key[2] == "Positive":
                        pos += j.value["count"]
                    if j.key[2] == "Negative":
                        neg += j.value["count"]
                    if j.key[2] == "Neutral":
                        neu += j.value["count"]
                    #test.append(total)
            if total == 0:
                new_item = {
                "pos": pos,
                "neu": neu,
                "neg": neg,
                "total": total,
                "neg%": 0,
                "neu%": 0,
                "pos%": 0,
                "ave_sen": sen/3,
                "ave_mag": mag/3
            }
            else:
             new_item = {
                "pos": pos,
                "neu": neu,
                "neg": neg,
                "total": total,
                "neg%": neg/total,
                "neu%": neu/total,
                "pos%": pos/total,
                "ave_sen": sen/3,
                "ave_mag": mag/3
            }
            data1[month] = new_item.copy()

        data2[i] = data1.copy()
    return data2
# postcode sentiment
@war_twitter_bp.route('/war/twitter/postcode_sentiment')
def get_points2():
    results = db.view('_design/sentiment/_view/war_postcode', group=True)
    results1 = db.view('_design/sentiment/_view/ave_sent_postcode', group=True)

    data = []
    all_postcode = []
    for row in results:
        if row.key[0] == None:
            continue
        if row.key[0] not in all_postcode:
            all_postcode.append(row.key[0])
        data.append(row)

    data1 = {}
    for i in all_postcode:
        neg = 0
        pos = 0
        neu = 0
        total = 0
        for j in data:
            if j.key[0] == i:
                total += j.value
                if j.key[1] == "Positive":
                    pos += j.value
                if j.key[1] == "Negative":
                    neg += j.value
                if j.key[1] == "Neutral":
                    neu += j.value
        for ii in results1:
            if ii.key == i:
                ave_sen = ii.value["sentiment"]
        new_item = {
            "war_neg%": neg/total,
            "war_neu%": neu/total,
            "war_pos%": pos/total,
            "war_ave_sen": ave_sen,
            "war_total": total,
        }
        data1[i] = new_item

    new_item1 = {
            "neg_sen": None,
            "pos_sen": None,
            "neu_sen": None,
            "total": None,
            "ave_sen": None,
            "neg%": None,
            "pos%": None,
            "neu%": None
        }
    
    for feature in sudo_postcode_geo['features']:
        if feature["properties"]["POA_NAME21"].lower() not in all_postcode:
            for j in new_item1:
                feature["properties"][j] = new_item1[j]
        for row in data1:
            if feature["properties"]["POA_NAME21"].lower() == row:
                for i in data1[row]:
                    feature["properties"][i] = data1[row][i]

    with open(f'{project_prefix}/back-end/geojson_output/war_postcode.json', 'w') as file:
        json.dump(sudo_postcode_geo, file)
    with open(f'{project_prefix}/back-end/geojson_output/war_postcode.json', 'r') as f:
        geo = json.load(f)
    return jsonify(geo)

# suburb sentiment
@war_twitter_bp.route('/war/twitter/suburb_sentiment')
def get_points3():
    results = db.view('_design/sentiment/_view/suburb_sentiment', group=True)

    data = []
    all_suburb = []
    for row in results:
        if row.key["suburb"] == None:
            continue
        if row.key["suburb"] not in all_suburb:
            all_suburb.append(row.key["suburb"].lower())
        data.append(row)

    data1 = {}
    for i in all_suburb:
        neg = 0
        pos = 0
        neu = 0
        total = 0
        mag = 0
        for j in data:
            if j.key["suburb"].lower() == i:
                total += j.value["count"]
                mag += j.value["average_magnitude"]
                if j.key["sentiment_category"] == "positive":
                    pos += j.value["count"]
                if j.key["sentiment_category"] == "negative":
                    neg += j.value["count"]
                if j.key["sentiment_category"] == "neutral":
                    neu += j.value["count"]

        new_item = {
            "war_total": total,
            "war_ave_mag": mag/3,
            "war_neg%": neg/total,
            "war_pos%": pos/total,
            "war_neu%": neu/total
        }
        data1[i] = new_item

    new_item1 = {
            "war_total": None,
            "war_ave_mag": None,
            "war_neg%": None,
            "war_pos%": None,
            "war_neu%": None
        }
    for feature in sudo_suburb_geo['features']:
        suburb_name = feature["properties"]["SAL_NAME21"].lower()
        suburb_name = re.sub(r'\([^)]*\)', '', suburb_name)
        suburb_name = suburb_name.replace(" ", "")
        if suburb_name not in all_suburb:
            for j in new_item1:
                feature["properties"][j] = new_item1[j]
        for row in data1:
            if row in suburb_name:
                for i in data1[row]:
                    feature["properties"][i] = data1[row][i]

    with open(f'{project_prefix}/back-end/geojson_output/war_suburb.json', 'w') as file:
        json.dump(sudo_suburb_geo, file)
    with open(f'{project_prefix}/back-end/geojson_output/war_suburb.json', 'r') as f:
        geo = json.load(f)
    """
    ww=0
    for g in geo["features"]:
        if g["properties"]["war_neu%"] is not None:
            ww+=1
    print(ww)
    """
    return jsonify(geo)

# sentiment_language
@war_twitter_bp.route('/war/twitter/sentiment_language')
def get_points4():
    results = db.view('_design/sentiment/_view/sentiment_language', group=True)
    
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
        num = 0
        mag = 0
        for j in data:
            if j.key[0] == i:
                total += j.value["count"]
                num += 1
                mag += j.value["average_magnitude"]
                if j.key[1] == "positive":
                    pos += j.value["count"]
                if j.key[1] == "negative":
                    neg += j.value["count"]
                if j.key[1] == "neutral":
                    neu += j.value["count"]
        
        new_item = {
            "pos": pos,
            "neu": neu,
            "neg": neg,
            "total": total,
            "neg%": neg/total,
            "neu%": neu/total,
            "pos%": pos/total,
            "ave_mag": mag/num
            
        }
        data1[i] = new_item
    return jsonify(data1)
