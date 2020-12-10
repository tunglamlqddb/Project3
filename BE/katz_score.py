import networkx as nx 
import numpy
import scipy.linalg
import math


G = nx.Graph()    
G.add_nodes_from([0,1,2,3])
elist = [(0,1), (1,2), (1,9)]
for i in elist:
    G.add_edge(i[0], i[1])
# G.add_edges_from([(0,1),(1,3)])
A = nx.adjacency_matrix(G)
print(A.todense())

# w, v = numpy.linalg.eigh(A.todense())
# lambda1 = max([abs(x) for x in w])
# I = numpy.eye(A.shape[0])
# S = None

# phi = (1+math.sqrt(5))/2.0
# print(lambda1)
# print(phi)
# S = numpy.linalg.inv(I - (1/lambda1-0.01) * A)
# print(S.sum(axis=1))

# centrality = nx.katz_centrality(G,1/lambda1-0.01, normalized=False) 
# for n,c in sorted(centrality.items()): 
#     print("%d %0.8f"%(n,c))

#def katz(list_nodes, )

