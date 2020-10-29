from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
from collections import defaultdict
import math
import csv
import pandas as pd
import json

app = Flask(__name__)
CORS(app)

# direct path of database
db_path = "/home/lam/Documents/prj3_copy/prj3/data/db.sqlite3"

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/query', methods=['POST'])
def query():
    num_records = request.get_json()["num_records"]

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        name1 = "paper_" + num_records
        name2 = "paper_authors_" + num_records
        name3 = "author_" + num_records
        check1 = "select count(name) from sqlite_master where type='table' and name='" + name1 + "'"
        check2 = "select count(name) from sqlite_master where type='table' and name='" + name2 + "'"
        check3 = "select count(name) from sqlite_master where type='table' and name='" + name3 + "'"
        
        create_paper_table = ("create table paper_" + num_records + 
                            " as select p.id, p.date from collab_paper p \
                            limit " + num_records
                            )

        create_paper_authors_table = ("create table paper_authors_" + num_records + 
                            " as select pa.* from collab_paper_authors pa \
                            where pa.paper_id in (select id from paper_" + num_records + ")"
                            )

        create_author_table = ("create table author_" + num_records + 
                            " as select a.id, a.affiliation_id, ins.university, a.country_id from collab_author a \
                              left join collab_institute ins \
                              on a.affiliation_id = ins.id  \
                            where a.id in (select distinct pa.author_id from paper_authors_" + num_records + " pa)"
                            )       

        cur.execute(check1)
        if cur.fetchone()[0] == 1:
            msg = "sub database already exists"
        else:
            cur.execute(check2)
            if cur.fetchone()[0] == 1:
                msg = "sub database already exists"
            else:
                cur.execute(check3)
                if cur.fetchone()[0] == 1:
                    msg = "sub database already exists" 

                else:
                    cur.execute(create_paper_table)
                    cur.execute(create_paper_authors_table)
                    cur.execute(create_author_table)
                    msg = "create sub database successfully!"
        

        # query to return result ==> may be unnecessary
        query1 = ("select * from paper_" + num_records)
        query2 = ("select * from author_" + num_records)
        query3 = ("select * from paper_authors_" + num_records)
        cur.execute(query1)
        name_1 = [i[0] for i in cur.description]
        res1 = cur.fetchall()
        cur.execute(query2)
        name_2 = [i[0] for i in cur.description]
        res2 = cur.fetchall()
        cur.execute(query3)
        name_3 = [i[0] for i in cur.description]
        res3 = cur.fetchall()
    return  json.dumps({"paper": [name_1,res1], "author": [name_2,res2], "paper_authors": [name_3,res3]})


@app.route('/create_co_authors', methods=['POST'])
def create_co_authors():
    num_records = request.get_json()["num_records"]

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        create_co_authors_table = ("create table co_author_" + num_records +
                                    " as select pa1.paper_id, pa1.author_id as id_author_1, pa2.author_id as id_author_2 \
                                    from paper_authors_" + num_records + " pa1 \
                                    join paper_authors_" + num_records + " pa2 \
                                    on pa1.paper_id = pa2.paper_id and pa1.author_id < pa2.author_id order by pa1.paper_id"
                                )       
        name = "co_author_" + num_records
        check = "select count(name) from sqlite_master where type='table' and name='" + name + "'"
        cur.execute(check)
        if cur.fetchone()[0] == 1:
            msg = "co author table " + num_records + " already exists"
            message ={ "msg" : msg, "name": name}
        else:
            res = cur.execute(create_co_authors_table)
            message = {"msg": "create co author table successfully!",
                    "name": name
            }

    # query to return result ==> may be unnecessary
    query = ("select * from co_author_" + num_records)
    cur.execute(query)
    column_names = [i[0] for i in cur.description]
    result = cur.fetchall()
    return json.dumps({"co_author": [column_names,result], "msg": message})


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
def create_potential_co_authors():
    num_records = request.get_json()["num_records"]
    level = request.get_json()["level"]
    #time_slice = request.get_json()["time_slice"]

    # name_of_sliced_co_author = slice_co_author(num_records, time_slice)

    temp = "co_author_" + num_records

    message = []
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        for i in range(level):
            if i > 0:
                temp = "potential_co_author_"+num_records+"_"+str(i)
            create_potential_co_authors_table = ("create table potential_co_author_" + num_records + "_" + str(i+1) 
                                            + " as \
                                                select co1.id_author_2 as id_author_1, co2.id_author_2 as id_author_2 \
                                                from " + temp + " co1 \
                                                join " + temp + " co2 \
                                                on co1.id_author_1 = co2.id_author_1 \
                                                and co1.id_author_2 < co2.id_author_2 \
                                                union \
                                                select co2.id_author_1 as id_author_1, co1.id_author_2 as id_author_2 \
                                                from " + temp + " co1 \
                                                join " + temp + " co2 \
                                                on co1.id_author_1 = co2.id_author_2 \
                                                and co1.id_author_2 > co2.id_author_1 \
                                                union \
                                                select co1.id_author_1 as id_author_1, co2.id_author_1 as id_author_2 \
                                                from " + temp + " co1 \
                                                join " + temp + " co2 \
                                                on co1.id_author_2 = co2.id_author_2 \
                                                and co1.id_author_1 < co2.id_author_1 \
                                                union \
                                                select co.id_author_1, co.id_author_2 \
                                                from " + temp + " co")
            name = "potential_co_author_" + num_records + "_" + str(i+1)
            check = "select count(name) from sqlite_master where type='table' and name='" + name + "'"
            cur.execute(check)
            if cur.fetchone()[0] == 1:
                msg = name + " already exists"
                message.append({"msg": msg})
            else:
                cur.execute(create_potential_co_authors_table)
                msg = "create potential co author table " + num_records + "_" + str(i+1) + " successfully"   
                message.append({"msg" : msg})
    
        message.append({"name": "potential_co_author_"+num_records+"_"+str(level)})

        # query to return result ==> may be unnecessary
        query = ("select * from potential_co_author_"+num_records+"_"+str(level))
        cur.execute(query)
        column_names = [i[0] for i in cur.description]
        result = cur.fetchall()
        return json.dumps({"last_potential": [column_names,result], "msg": message})


def create_author_graph(num_records):
    adj = defaultdict(dict)
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        get_co_author = ("select co.id_author_1, co.id_author_2 from co_author_" + num_records + " co")
        cur.execute(get_co_author)
        records = cur.fetchall()
        for row in records:
            u = row[0]
            v = row[1]
            if v in adj[u]:
                adj[u][v] += 1
                adj[v][u] += 1
            else:
                adj[u][v] = 1
                adj[v][u] = 1 
    return adj

def CommonNeighbor(id1, id2, adj):
    return len(set(adj[id1].keys()) & set(adj[id2].keys()))

def AdamicAdar(id1, id2, adj):
    # {z}
    common_neighbors = list(set(adj[id1].keys()) & set(adj[id2].keys()))
    res = 0
    for z in common_neighbors:
        res += 1 / (math.log(len(adj[z])))
    return res

def JaccardCoefficient(id1, id2, adj):
    return len(set(adj[id1].keys()) & set(adj[id2].keys())) / len(set(adj[id1].keys()).union(set(adj[id2].keys())))

def PreferentialAttachment(id1, id2, adj):
    return len(adj[id1]) * len(adj[id2])

def ResourceAllocation(id1, id2, adj):
    # {z}
    common_neighbors = list(set(adj[id1].keys()) & set(adj[id2].keys()))
    res = 0
    for z in common_neighbors:
        res += 1 / len(adj[z])
    return res

def bfs(s, e, adj):     #bfs to find shortest path between s and e
    num_ver = len(adj)
    max_num_edges = num_ver * (num_ver-1) / 2
    if s == e: return 0
    visited = dict.fromkeys(adj.keys(), False)
    dist = dict.fromkeys(adj.keys(), max_num_edges)
    q = []
    q.append(s)
    visited[s] = True
    dist[s] = 0
    while q:
        u = q.pop(0)
        if u == e: return dist[u]
        else:
            for v in adj[u].keys():
                if visited[v] == False:
                    q.append(v)
                    visited[v] = True  
                    dist[v] = dist[u] + 1
    return 0

def ShortestPath(id1, id2, adj):
    dist = bfs(id1, id2, adj)
    if dist == 0: return 0
    else:
        return 1 / dist

def CommonCountry(adj, records, num_records):  # records of potential_co_author
    # return list of scores of each pair
    CommonCountry_list = []
    # dict to store university and country_id of each author
    affili = defaultdict(list)

    # query country_id and affiliation_id of 2 authors
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor() 
        cur.execute("select a.id, a.university, a.country_id \
                    from author_" + num_records + " a")
         
        res = cur.fetchall()
        for row in res:
            if row[1] == None and row[2] == None:
                affili[row[0]] = ["None", -1]
            if row[1] == None and row[2] != None:
                affili[row[0]] = ["None", row[2]]
            if row[2] == None and row[1] != None:
                affili[row[0]] = [row[1], -1]   
            if row[2] != None and row[1] != None:
                affili[row[0]] = [row[1], row[2]]
        
    def sim_work_2(id1, id2):
        if affili[id1][0] == affili[id2][0]: return 2
        elif affili[id1][1] == affili[id2][1]: return 1
        else: return  0

    def sim_work_3(id1, id2, id3):
        if affili[id1][0] == affili[id2][0] and affili[id1][0] == affili[id3][0]:
            return 2
        elif affili[id1][1] == affili[id2][1] and affili[id1][1] == affili[id3][1]:
            return 1
        else: return 0
    
    for row in records:
        id1 = row[0]
        id2 = row[1]
        comm_country_score = sim_work_2(id1, id2)    
        common_neighbors = list(set(adj[id1].keys()) & set(adj[id2].keys()))
        for z in common_neighbors:
            comm_country_score += sim_work_3(id1, id2, z)
        CommonCountry_list.append(comm_country_score)
    return CommonCountry_list
    
   
@app.route('/calculate_scores', methods=['POST'])
def calculate_scores():
    num_records = request.get_json()["num_records"]
    level = request.get_json()["level"]
    #time_slice = request.get_json()["time_slice"]

    adj = create_author_graph(num_records)

    #lists to store scores
    CommonNeighbor_list = []
    AdamicAdar_list = []
    JaccardCoefficient_list = []
    PreferentialAttachment_list = []
    ResourceAllocation_list = []
    ShortestPath_list = []
    CommonCountry_list = []

    # labels
  #  labels = []

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        potential_co_author_query = "select * from potential_co_author_" + num_records + "_" + str(level)
        cur.execute(potential_co_author_query)
        records = cur.fetchall()
        # calculate CommonCountry score seperately
        CommonCountry_list = CommonCountry(adj, records, num_records)
        # calculate other scores in iterative manner
        for result in records:
            id1 = result[0]
            id2 = result[1]
            CommonNeighbor_list.append(CommonNeighbor(id1,id2,adj))
            AdamicAdar_list.append(AdamicAdar(id1,id2,adj))
            JaccardCoefficient_list.append(JaccardCoefficient(id1,id2,adj))
            PreferentialAttachment_list.append(PreferentialAttachment(id1,id2,adj))
            ResourceAllocation_list.append(ResourceAllocation(id1,id2,adj))
            ShortestPath_list.append(ShortestPath(id1,id2, adj))
        
        with open("..\Results\final_potential_co_authors_" + num_records + ".csv", "w") as f:
            csv_out = csv.writer(f)
            # write header
            csv_out.writerow([d[0] for d in cur.description])
            # write data
            csv_out.writerows(records)
                    
        # using pandas to add score columns
        
        df = pd.read_csv("..\Results\final_potential_co_authors_" + num_records + ".csv")
        df['CommonNeighbor'] = CommonCountry_list
        df['AdamicAdar'] = AdamicAdar_list
        df['JaccardCoefficient'] = JaccardCoefficient_list
        df['PreferentialAttachment'] = PreferentialAttachment_list
        df['ResourceAllocation'] = ResourceAllocation_list
        df['ShortestPath'] = ShortestPath_list
        df['CommonCountry'] = CommonCountry_list
       # df['Label'] = labels
        df.to_csv("..\Results\final_potential_co_authors_" + num_records + ".csv", index=False)

    return jsonify({"msg": "done", "name" : "final_potential_co_authors" + num_records + " .csv", "check" : df['CommonNeighbor'].to_json()})       


app.run(debug=True)
