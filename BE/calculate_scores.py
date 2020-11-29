import sqlite3
import csv
import pandas as pd
import json, os
from flask import jsonify
# direct path of database
# db_path = "/home/lam/Documents/prj3_copy/prj3/data/db.sqlite3"
basedir = os.path.dirname((os.path.dirname(__file__)))
results_path = os.path.join(basedir, 'Results')
par_prj = os.path.dirname(basedir)
db_path = os.path.join(par_prj, 'Data_Project3')

from define_scores import CommonNeighbor, AdamicAdar, JaccardCoefficient, PreferentialAttachment, ResourceAllocation, ShortestPath, CommonCountry

def calculate_scores_dynamic(num_records, level, topic, weight_type, graph):
    adj = graph.adj
    time_patterns = graph.time_patterns

    #lists to store scores
    CommonNeighbor_list = []
    AdamicAdar_list = []
    JaccardCoefficient_list = []
    PreferentialAttachment_list = []
    ResourceAllocation_list = []
    ShortestPath_list = []
    CommonCountry_list = []

    # labels
    labels = []

    # use for (u,v) not co author 
    max_time = max(time_patterns) + 1

    with sqlite3.connect(db_path + "/subDB_" + num_records + "_" + topic + ".sqlite3") as conn:
        cur = conn.cursor()
        potential_co_author_query = "select * from potential_co_author_" + str(level) 
        cur.execute(potential_co_author_query)
        records = cur.fetchall()
        # calculate CommonCountry score seperately
        CommonCountry_list = CommonCountry(adj, graph.list_vertices, records, num_records, topic, max_time, "dynamic", max_time)
        # calculate other scores in iterative manner
        for result in records:
            id1 = result[0]
            id2 = result[1]
            if id2 in adj[id1].keys(): 
                t = max(adj[id1][id2].years)
                CommonNeighbor_list.append(CommonNeighbor(id1,id2,adj, t)[weight_type])
                AdamicAdar_list.append(AdamicAdar(id1,id2,adj, t)[weight_type])
                JaccardCoefficient_list.append(JaccardCoefficient(id1,id2,adj, t)[weight_type])
                PreferentialAttachment_list.append(PreferentialAttachment(id1,id2,adj, t)[weight_type])
                ResourceAllocation_list.append(ResourceAllocation(id1,id2,adj, t)[weight_type])
                ShortestPath_list.append(ShortestPath(id1,id2, adj, t))
                labels.append(1)
            else:
                CommonNeighbor_list.append(CommonNeighbor(id1,id2,adj, max_time)[weight_type])
                AdamicAdar_list.append(AdamicAdar(id1,id2,adj, max_time)[weight_type])
                JaccardCoefficient_list.append(JaccardCoefficient(id1,id2,adj, max_time)[weight_type])
                PreferentialAttachment_list.append(PreferentialAttachment(id1,id2,adj, max_time)[weight_type])
                ResourceAllocation_list.append(ResourceAllocation(id1,id2,adj, max_time)[weight_type])
                ShortestPath_list.append(ShortestPath(id1,id2, adj, max_time))
                labels.append(0)
            
        file_name = results_path + "/Final_potential_co_authors_" + num_records + "_" + topic + "_" + weight_type + "_dynamic.csv"
        write_scores_to_csv(file_name, CommonCountry_list, AdamicAdar_list, JaccardCoefficient_list, PreferentialAttachment_list, ResourceAllocation_list, ShortestPath_list, CommonCountry_list, labels, cur, records)

    return jsonify({"msg": "done", "name" : file_name})       

def calculate_scores_static(num_records, level, topic, weight_type, graph, time_slice):
    adj = graph.adj
    time_patterns = graph.time_patterns
    max_time = max(time_patterns) + 1

    #lists to store scores
    CommonNeighbor_list = []
    AdamicAdar_list = []
    JaccardCoefficient_list = []
    PreferentialAttachment_list = []
    ResourceAllocation_list = []
    ShortestPath_list = []
    CommonCountry_list = []

    # labels
    labels = []

    with sqlite3.connect(db_path + "/subDB_" + num_records + "_" + topic + ".sqlite3") as conn:
        cur = conn.cursor()
        potential_co_author_query = "select * from potential_co_author_" + str(level) 
        cur.execute(potential_co_author_query)
        records = cur.fetchall()
        # get co authors after time slice as labels
        after_time_sliced_table_query = ("select co.id_author_1, co.id_author_2 \
                                        from co_author co \
                                        join paper p \
                                        on co.paper_id = p.id \
                                        where substr(p.date,1,4) > " + time_slice
                                        )
        cur.execute(after_time_sliced_table_query)
        after_time_sliced_result = cur.fetchall()
        # calculate CommonCountry score seperately
        CommonCountry_list = CommonCountry(adj, graph.list_vertices, records, num_records, topic, max_time, "static", max_time)
        for row in records:
            id1 = row[0]
            id2 = row[1]
            CommonNeighbor_list.append(CommonNeighbor(id1,id2,adj, max_time)[weight_type])
            AdamicAdar_list.append(AdamicAdar(id1,id2,adj, max_time)[weight_type])
            JaccardCoefficient_list.append(JaccardCoefficient(id1,id2,adj, max_time)[weight_type])
            PreferentialAttachment_list.append(PreferentialAttachment(id1,id2,adj, max_time)[weight_type])
            ResourceAllocation_list.append(ResourceAllocation(id1,id2,adj, max_time)[weight_type])
            ShortestPath_list.append(ShortestPath(id1,id2, adj, max_time))
            if (id1, id2) in after_time_sliced_result:
                labels.append(1)
            else: labels.append(0)

        file_name = results_path + "/Final_potential_co_authors_" + num_records + "_" + topic + "_" + weight_type + "_static.csv"
        write_scores_to_csv(file_name, CommonCountry_list, AdamicAdar_list, JaccardCoefficient_list, PreferentialAttachment_list, ResourceAllocation_list, ShortestPath_list, CommonCountry_list, labels, cur, records)

    return jsonify({"msg": "done", "name" : file_name}) 

def write_scores_to_csv(file_name, CommonNeighbor_list, AdamicAdar_list, JaccardCoefficient_list, PreferentialAttachment_list, ResourceAllocation_list, ShortestPath_list, CommonCountry_list, labels, cur, records):
    with open(file_name, "w") as f:
        csv_out = csv.writer(f)
        # write header
        csv_out.writerow([d[0] for d in cur.description])
        # write data
        csv_out.writerows(records)
                
    # using pandas to add score columns
        
    df = pd.read_csv(file_name)
    df['CommonNeighbor'] = CommonCountry_list
    df['AdamicAdar'] = AdamicAdar_list
    df['JaccardCoefficient'] = JaccardCoefficient_list
    df['PreferentialAttachment'] = PreferentialAttachment_list
    df['ResourceAllocation'] = ResourceAllocation_list
    df['ShortestPath'] = ShortestPath_list
    df['CommonCountry'] = CommonCountry_list
    df['Label'] = labels
    df.to_csv(file_name, index=False)