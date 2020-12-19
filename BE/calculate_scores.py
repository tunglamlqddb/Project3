import sqlite3
import csv
import pandas as pd
import json, os
from datetime import datetime
from flask import jsonify
from query import create_potential_co_authors
# direct path of database
# db_path = "/home/lam/Documents/prj3_copy/prj3/data/db.sqlite3"
basedir = os.path.dirname((os.path.dirname(__file__)))
results_path = os.path.join(basedir, 'Results')
par_prj = os.path.dirname(basedir)
db_path = os.path.join(par_prj, 'Data_Project3')

from define_scores import list_co_authors_before_t, CommonNeighbor, AdamicAdar, JaccardCoefficient, PreferentialAttachment, ResourceAllocation, ShortestPath, CommonCountry

def calculate_scores_dynamic(num_records, level, topics, from_date, to_date, weight_type, graph, csv_file_name):
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
    log_cnt = 0
    with sqlite3.connect(db_path + "/subDB_" + num_records + "_" + "_".join(topics) + "_" + from_date + "_" + to_date + ".sqlite3") as conn:
        cur = conn.cursor()
        potential_co_author_query = "select * from potential_co_author_" + str(level) 
        cur.execute(potential_co_author_query)
        records = cur.fetchall()
        # calculate CommonCountry score seperately
        CommonCountry_list = CommonCountry(adj, graph.list_vertices, records, max_time, "dynamic", max_time)
        # calculate other scores in iterative manner
        cnt_1 = 0
        cnt_0 = 0
        
        for result in records:
            id1 = result[0]
            id2 = result[1]
            if id2 in adj[id1].keys(): 
                t = adj[id1][id2].max_time_pattern
                co_id1 = list_co_authors_before_t(id1, t, adj)
                co_id2 = list_co_authors_before_t(id2, t, adj)
                common_neighbors = list(set(co_id1) & set(co_id2))
                CommonNeighbor_list.append(CommonNeighbor(id1,id2,adj,t,co_id1,co_id2, common_neighbors, weight_type))
                AdamicAdar_list.append(AdamicAdar(id1,id2,adj,t,co_id1,co_id2,common_neighbors, weight_type))
                JaccardCoefficient_list.append(JaccardCoefficient(id1,id2,adj,t,co_id1,co_id2,common_neighbors, weight_type))
                PreferentialAttachment_list.append(PreferentialAttachment(id1,id2,adj,t,co_id1,co_id2,common_neighbors, weight_type))
                ResourceAllocation_list.append(ResourceAllocation(id1,id2,adj,t,co_id1,co_id2,common_neighbors, weight_type))
                ShortestPath_list.append(ShortestPath(id1,id2, adj, t))
                labels.append(1)
                cnt_1 += 1    
            else:
                co_id1 = list_co_authors_before_t(id1, max_time, adj)
                co_id2 = list_co_authors_before_t(id2, max_time, adj)
                common_neighbors = list(set(co_id1) & set(co_id2))
                CommonNeighbor_list.append(CommonNeighbor(id1,id2,adj, max_time,co_id1,co_id2,common_neighbors, weight_type))
                AdamicAdar_list.append(AdamicAdar(id1,id2,adj, max_time,co_id1,co_id2,common_neighbors, weight_type))
                JaccardCoefficient_list.append(JaccardCoefficient(id1,id2,adj, max_time,co_id1,co_id2,common_neighbors, weight_type))
                PreferentialAttachment_list.append(PreferentialAttachment(id1,id2,adj, max_time,co_id1,co_id2,common_neighbors, weight_type))
                ResourceAllocation_list.append(ResourceAllocation(id1,id2,adj, max_time,co_id1,co_id2,common_neighbors, weight_type))
                ShortestPath_list.append(ShortestPath(id1,id2, adj, max_time))
                labels.append(0)
                cnt_0 += 1
            log_cnt += 1
            if log_cnt % 5000 == 0:
                print("Done {}--{}---{}".format(id1, id2, datetime.now().time()))
        if csv_file_name == "":
            file_name = results_path + "/Data_" + num_records + "_" + "_".join(topics) + "_" + from_date + "_" + to_date + "_" + weight_type + "_dynamic.csv"
        else: 
            file_name = results_path + "/" + csv_file_name + ".csv"
        write_scores_to_csv(file_name, CommonCountry_list, AdamicAdar_list, JaccardCoefficient_list, PreferentialAttachment_list, ResourceAllocation_list, ShortestPath_list, CommonCountry_list, labels, cur, records)

    return jsonify({"msg": (cnt_0, cnt_1), "name" : file_name})       

def calculate_scores_static(num_records, level, topics, from_date, to_date, weight_type, graph, time_slice, csv_file_name):
    adj = graph.adj
    time_patterns = graph.time_patterns
    max_time = max(time_patterns) + 1
    ymd = time_slice.split('-')
    if len(ymd) == 3:
        time_slice = "-".join(ymd)
    elif len(ymd) == 2:
        time_slice = "-".join(ymd) + "-31"
    elif len(ymd) == 1:
        time_slice = "-".join(ymd) + "-12-31"

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

    with sqlite3.connect(db_path + "/subDB_" + num_records + "_" + "_".join(topics) + "_" + from_date + "_" + to_date + ".sqlite3") as conn:
        cur = conn.cursor()
        before_time_sliced_table_query = ("create table co_author_before_time_slice_" + "".join(ymd) + " as \
                                        select co.id_author_1, co.id_author_2 \
                                        from co_author co \
                                        join paper p \
                                        on co.paper_id = p.id \
                                        where p.date <= '" + time_slice + "'"   
                                        )
        co_author_name = "co_author_before_time_slice_" + "".join(ymd)
        check = "select count(name) from sqlite_master where type='table' and name='" + co_author_name + "'"
        cur.execute(check)
        if cur.fetchone()[0] == 1:
            msg = co_author_name + " already exists"
        else:
            cur.execute(before_time_sliced_table_query)

        potential_co_author_name = "potential_co_author_before_time_slice_" + "".join(ymd) + "_"
        res = create_potential_co_authors(num_records, level, topics, co_author_name, potential_co_author_name)
        records = json.loads(res)['last_potential'][1]
        # get co authors after time slice as labels
        after_time_sliced_table_query = ("select co.id_author_1, co.id_author_2 \
                                        from co_author co \
                                        join paper p \
                                        on co.paper_id = p.id \
                                        where p.date > '" + time_slice + "'"
                                        )
        cur.execute(after_time_sliced_table_query)
        after_time_sliced_result = cur.fetchall()
        # calculate CommonCountry score seperately
        CommonCountry_list = CommonCountry(adj, graph.list_vertices, records, max_time, "static", max_time)
        cnt_0 = 0
        cnt_1 = 0
        for row in records:
            id1 = row[0]
            id2 = row[1]
            co_id1 = list_co_authors_before_t(id1, max_time, adj)
            co_id2 = list_co_authors_before_t(id2, max_time, adj)
            common_neighbors = list(set(co_id1) & set(co_id2))
            CommonNeighbor_list.append(CommonNeighbor(id1,id2,adj, max_time,co_id1,co_id2,common_neighbors, weight_type))
            AdamicAdar_list.append(AdamicAdar(id1,id2,adj, max_time,co_id1,co_id2,common_neighbors, weight_type))
            JaccardCoefficient_list.append(JaccardCoefficient(id1,id2,adj, max_time,co_id1,co_id2,common_neighbors, weight_type))
            PreferentialAttachment_list.append(PreferentialAttachment(id1,id2,adj, max_time,co_id1,co_id2,common_neighbors, weight_type))
            ResourceAllocation_list.append(ResourceAllocation(id1,id2,adj, max_time,co_id1,co_id2,common_neighbors, weight_type))
            ShortestPath_list.append(ShortestPath(id1,id2, adj, max_time))
            if (id1, id2) in after_time_sliced_result:
                labels.append(1)
                cnt_1 += 1
            else: 
                labels.append(0)
                cnt_0 += 1
        if csv_file_name == "":
            file_name = results_path + "/Data_" + num_records + "_" + "_".join(topics) + "_" + from_date + "_" + to_date + "_" + weight_type + "_" + time_slice + "_static.csv"
        else: 
            file_name = results_path + "/" + csv_file_name + ".csv"
        write_scores_to_csv(file_name, CommonCountry_list, AdamicAdar_list, JaccardCoefficient_list, PreferentialAttachment_list, ResourceAllocation_list, ShortestPath_list, CommonCountry_list, labels, cur, records)

    return jsonify({"msg": (cnt_0, cnt_1), "name" : file_name}) 

def write_scores_to_csv(file_name, CommonNeighbor_list, AdamicAdar_list, JaccardCoefficient_list, PreferentialAttachment_list, ResourceAllocation_list, ShortestPath_list, CommonCountry_list, labels, cur, records):
    with open(file_name, "a+") as f:
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