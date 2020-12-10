import math

def list_co_authors_before_t(u, t, adj):
    res = []
    for v in adj[u].keys():
        for year in adj[u][v].years:
            if year <= t:
                res.append(v)
                break
    return res

def get_weight_before_t(u,v,adj,t):
    old_w = adj[u][v].w
    cnt = 0
    for year in adj[u][v].years:
        if year > t: 
            cnt += 1
    return old_w - cnt

def CommonNeighbor(id1, id2, adj, t):
    co_id1 = list_co_authors_before_t(id1, t, adj)
    co_id2 = list_co_authors_before_t(id2, t, adj)
    common_neighbors = list(set(co_id1) & set(co_id2))
    # unweighted
    unweighted_res = len(set(co_id1) & set(co_id2))
    # weighted
    weighted_res = 0
    for z in common_neighbors:
        w_id1_z = get_weight_before_t(id1, z, adj, t)
        w_id2_z = get_weight_before_t(id2, z, adj, t)
        weighted_res += (w_id1_z + w_id2_z) / 2
    return {'unweighted' : unweighted_res, 'weighted' : weighted_res}

def AdamicAdar(id1, id2, adj, t):
    # {z}
    co_id1 = list_co_authors_before_t(id1, t, adj)
    co_id2 = list_co_authors_before_t(id2, t, adj)
    common_neighbors = list(set(co_id1) & set(co_id2))
    # unweighted
    unweighted_res = 0
    for z in common_neighbors:
        co_z = list_co_authors_before_t(z, t, adj)
        unweighted_res += 1 / (math.log(len(co_z)))
    # weighted
    weighted_res = 0
    for z in common_neighbors:
        w_id1_z = get_weight_before_t(id1, z, adj, t)
        w_id2_z = get_weight_before_t(id2, z, adj, t)
        numerator = (w_id1_z + w_id2_z) / 2
        denominator = 0
        co_z = list_co_authors_before_t(z, t, adj)
        for i in co_z:
            w_z_i = get_weight_before_t(z, i, adj, t)
            denominator += w_z_i
        denominator = math.log(denominator)
        weighted_res += numerator / denominator
    return {'unweighted' : unweighted_res, 'weighted' : weighted_res}

def JaccardCoefficient(id1, id2, adj, t):
    co_id1 = list_co_authors_before_t(id1, t, adj)
    co_id2 = list_co_authors_before_t(id2, t, adj)
    # unweighted
    unweighted_res = len(set(co_id1) & set(co_id2)) / len(set(co_id1).union(set(co_id2)))
    # weighted
    weighted_res = unweighted_res
    return {'unweighted' : unweighted_res, 'weighted' : weighted_res}

def PreferentialAttachment(id1, id2, adj, t):
    co_id1 = list_co_authors_before_t(id1, t, adj)
    co_id2 = list_co_authors_before_t(id2, t, adj)
    # unweighted
    unweighted_res = len(co_id1) * len(co_id2)
    # weighted
    id1_contri = 0
    for z in co_id1:
        w_id1_z = get_weight_before_t(id1, z, adj, t)
        id1_contri += w_id1_z
    id1_contri /= len(co_id1)

    id2_contri = 0
    for z in co_id2:
        w_id2_z = get_weight_before_t(id2, z, adj, t)
        id2_contri += w_id2_z
    id2_contri /= len(co_id2)
    weighted_res = id1_contri * id2_contri
    return {'unweighted' : unweighted_res, 'weighted' : weighted_res}

def ResourceAllocation(id1, id2, adj, t):
    # {z}
    co_id1 = list_co_authors_before_t(id1, t, adj)
    co_id2 = list_co_authors_before_t(id2, t, adj)
    common_neighbors = list(set(co_id1) & set(co_id2))
    # unweighted
    unweighted_res = 0
    for z in common_neighbors:
        co_z = list_co_authors_before_t(z, t, adj)
        unweighted_res += 1 / (len(co_z))
    # weighted
    weighted_res = 0
    for z in common_neighbors:
        w_id1_z = get_weight_before_t(id1, z, adj, t)
        w_id2_z = get_weight_before_t(id2, z, adj, t)
        numerator = (w_id1_z + w_id2_z) / 2
        co_z = list_co_authors_before_t(z, t, adj)
        denominator = 0
        for i in co_z:
            w_z_i = get_weight_before_t(z, i, adj, t)
            denominator += w_z_i
        weighted_res += numerator / denominator
    return {'unweighted' : unweighted_res, 'weighted' : weighted_res}

def bfs(s, e, adj, t):     #bfs to find shortest path between s and e
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
        co_u = list_co_authors_before_t(u, t, adj)
        if u == e: return dist[u]
        else:
            for v in co_u:
                if visited[v] == False:
                    q.append(v)
                    visited[v] = True  
                    dist[v] = dist[u] + 1
    return 0

def ShortestPath(id1, id2, adj,t):    # not take w into account
    dist = bfs(id1, id2, adj, t)
    if dist == 0: return 0
    else:   
        return 1 / dist

def CommonCountry(adj, list_vertices, records, num_records, max_time, label_type, time_slice):  # records of potential_co_author
    # return list of scores of each pair
    CommonCountry_list = []
    
    # query country_id and affiliation_id of 2 authors
        
    def sim_work_2(id1, id2):
        if list_vertices[id1].university == list_vertices[id2].university: return 2
        elif list_vertices[id1].country_id == list_vertices[id2].country_id: return 1
        else: return  0

    def sim_work_3(id1, id2, id3):
        if list_vertices[id1].university == list_vertices[id2].university and list_vertices[id1].university == list_vertices[id3].university:
            return 2
        elif list_vertices[id1].country_id == list_vertices[id2].country_id and list_vertices[id1].country_id == list_vertices[id3].country_id:
            return 1
        else: return 0
    if label_type == "dynamic":
        for row in records:
            id1 = row[0]
            id2 = row[1]
            if id2 in adj[id1].keys(): 
                t = max(adj[id1][id2].years)
                comm_country_score = sim_work_2(id1, id2)    
                co_id1 = list_co_authors_before_t(id1, t, adj)
                co_id2 = list_co_authors_before_t(id2, t, adj)
                common_neighbors = list(set(co_id1) & set(co_id2))
                for z in common_neighbors:
                    comm_country_score += sim_work_3(id1, id2, z)
                CommonCountry_list.append(comm_country_score)
            else:
                comm_country_score = sim_work_2(id1, id2)    
                co_id1 = list_co_authors_before_t(id1, max_time, adj)
                co_id2 = list_co_authors_before_t(id2, max_time, adj)
                common_neighbors = list(set(co_id1) & set(co_id2))
                for z in common_neighbors:
                    comm_country_score += sim_work_3(id1, id2, z)
                CommonCountry_list.append(comm_country_score)
    if label_type == "static":
        for row in records:
            id1 = row[0]
            id2 = row[1]
            comm_country_score = sim_work_2(id1, id2)    
            co_id1 = list_co_authors_before_t(id1, time_slice, adj)
            co_id2 = list_co_authors_before_t(id2, time_slice, adj)
            common_neighbors = list(set(co_id1) & set(co_id2))
            for z in common_neighbors:
                comm_country_score += sim_work_3(id1, id2, z)
            CommonCountry_list.append(comm_country_score)
            
    return CommonCountry_list


