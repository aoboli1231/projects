import json
from flask import Flask, jsonify
from flask import Blueprint

geojson_bp = Blueprint('geojson', __name__)
app = Flask(__name__)

project_prefix = '/home/ubuntu/ccc'

# merge geojson for state


@geojson_bp.route('/state_geojson')
def merge_geojson_state():
    with open(f'{project_prefix}/back-end/geojson_output/war_state.json', 'r') as f:
        war_state_geo = json.load(f)
    with open(f'{project_prefix}/back-end/geojson_output/lgbt_state.json', 'r') as f2:
        lgbt_state_geo = json.load(f2)
    with open(f'{project_prefix}/back-end/geojson_output/general_state.json', 'r') as f3:
        general_state_geo = json.load(f3)

    features1 = war_state_geo['features']
    properties1 = [feature['properties'] for feature in features1]

    features2 = lgbt_state_geo['features']
    properties2 = [feature['properties'] for feature in features2]

    features3 = general_state_geo['features']
    properties3 = [feature['properties'] for feature in features3]

    merged_properties = []
    for prop1, prop2, prop3 in zip(properties1, properties2, properties3):
        merged_prop = {**prop1, **prop2, **prop3}
        merged_properties.append(merged_prop)

    merged_features = []
    for feature, merged_prop in zip(features1, merged_properties):
        new_feature = {
            'type': 'Feature',
            'geometry': feature['geometry'],
            'properties': merged_prop
        }
        merged_features.append(new_feature)

    merged_geojson = {
        'type': 'FeatureCollection',
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        'features': merged_features
    }
    with open(f"{project_prefix}/back-end/geojson_output/output_state_geojson_file", 'w') as output:
        json.dump(merged_geojson, output)
    with open(f"{project_prefix}/back-end/geojson_output/output_state_geojson_file", 'r') as ff:
        state_geo = json.load(ff)
    return jsonify(state_geo)

# merge geojson for suburb


@geojson_bp.route('/suburb_geojson')
def merge_geojson_suburb():
    with open(f'{project_prefix}/back-end/geojson_output/war_suburb.json', 'r') as f:
        war_suburb_geo = json.load(f)
    with open(f'{project_prefix}/back-end/geojson_output/lgbt_suburb.json', 'r') as f2:
        lgbt_suburb_geo = json.load(f2)
    with open(f'{project_prefix}/back-end/geojson_output/general_suburb.json', 'r') as f3:
        general_suburb_geo = json.load(f3)

    features1 = war_suburb_geo['features']
    properties1 = [feature['properties'] for feature in features1]

    features2 = lgbt_suburb_geo['features']
    properties2 = [feature['properties'] for feature in features2]

    features3 = general_suburb_geo['features']
    properties3 = [feature['properties'] for feature in features3]

    merged_properties = []

    for prop1, prop2, prop3 in zip(properties1, properties2, properties3):
        merged_prop = {**prop1, **prop2, **prop3}
        merged_properties.append(merged_prop)

    merged_features = []
    for feature, merged_prop in zip(features1, merged_properties):
        new_feature = {
            'type': 'Feature',
            'geometry': feature['geometry'],
            'properties': merged_prop
        }
        merged_features.append(new_feature)

    merged_geojson = {
        'type': 'FeatureCollection',
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        'features': merged_features
    }
    with open(f"{project_prefix}/back-end/geojson_output/output_suburb_geojson_file", 'w') as output:
        json.dump(merged_geojson, output)
    with open(f"{project_prefix}/back-end/geojson_output/output_suburb_geojson_file", 'r') as ff:
        suburb_geo = json.load(ff)
    return jsonify(suburb_geo)
