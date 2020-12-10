from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
from collections import defaultdict
import math
import csv
import pandas as pd
import json, os

from calculate_scores import calculate_scores_dynamic, calculate_scores_static
from query import create_sub_tables, create_co_authors, create_potential_co_authors, get_dates_of_topics
from co_author_graph import *

app = Flask(__name__)
CORS(app)

# direct path of database
# db_path = "/home/lam/Documents/prj3_copy/prj3/data/db.sqlite3"
basedir = os.path.dirname(os.path.dirname((os.path.dirname(__file__))))
db_path = os.path.join(basedir, 'Data_Project3')

@app.route('/get_dates_of_topics', methods=['POST'])
def _get_dates_of_topics():
    topics = request.get_json()["topics"]
    return get_dates_of_topics(topics)

@app.route('/query', methods=['POST'])
def _query():
    num_records = request.get_json()["num_records"]
    topics = request.get_json()["topics"]
    from_date = request.get_json()["from_date"]
    to_date = request.get_json()["to_date"]
    return create_sub_tables(num_records, topics, from_date, to_date)
    

@app.route('/create_co_authors', methods=['POST'])
def _create_co_authors():
    num_records = request.get_json()["num_records"]
    topics = request.get_json()["topics"]
    return create_co_authors(num_records, topics)
   
# def slice_co_author(num_records, time_slice):
#     # return name of sliced_co_author table
#     with sqlite3.connect("/home/lam/Documents/prj3_copy/prj3/data/db.sqlite3") as conn:
#         cur = conn.cursor()
#         name = "sliced_co_author_" + num_records + "_" + time_slice.replace('-', '')
#         sliced_query = (" create table " + name + " as \
#                           select co.paper_id, co.id_author_1, co.id_author_2 from co_author_" + num_records + " co \
#                           join paper_" + num_records + " p \
#                           on co.paper_id = p.id \
#                           where p.date < '" + time_slice + "'")
#         check = ("select count(name) from sqlite_master where type='table' and name='" + name + "'")
#         cur.execute(check)
#         if cur.fetchone()[0] == 0:
#             cur.execute(sliced_query)
#     return name

@app.route('/create_potential_co_authors', methods=['POST'])
def _create_potential_co_authors():
    num_records = request.get_json()["num_records"]
    level = request.get_json()["level"]
    topics = request.get_json()["topics"]
    return create_potential_co_authors(num_records, level, topics, "co_author", "potential_co_author_")
    #time_slice = request.get_json()["time_slice"]

    # name_of_sliced_co_author = slice_co_author(num_records, time_slice)
    
   
@app.route('/calculate_scores', methods=['POST'])
def _calculate_scores():
    # time is divided by year
    num_records = request.get_json()["num_records"]
    level = request.get_json()["level"]
    topics = request.get_json()["topics"]
    weight_type = request.get_json()["weight_type"]
    label_type = request.get_json()["label_type"]
    graph = Co_Author_Graph(num_records, topics, db_path + "/subDB_" + num_records + "_" + "_".join(topics) + ".sqlite3")
    if label_type == "dynamic":
        return calculate_scores_dynamic(num_records, level, topics, weight_type, graph)
    if label_type == "static":
        time_slice = request.get_json()["time_slice"]
        return calculate_scores_static(num_records, level, topics, weight_type, graph, time_slice)

  

app.run(debug=True)
