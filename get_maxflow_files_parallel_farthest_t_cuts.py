from __future__ import division
from itertools import chain
import networkx as nx 
import argparse 
import multiprocessing as mp  


def compute_op_scores(cur_node, Gcapacity, Gicapacity,Gunw):
    
    final_op_strs=[]
    #finding the target which is geometrically farthest number of hops away from a particular source node (unweighted graph used for this step)
    farthest_target=[]
    farthest_hops = 0
    #unweighted and weighted dijkstras
    length, path = nx.single_source_dijkstra(Gunw, cur_node)
    w_length,w_path = nx.single_source_dijkstra(Gicapacity, cur_node, weight="weight")

    #list of farthest target nodes
    r_plengths = length.items()
    for node in r_plengths:
        if node[1] > farthest_hops:
            farthest_hops = node[1]
    for node in r_plengths:
        if node[1] == farthest_hops:
            farthest_target.append(node[0])
   
    #retrieving scores for the topologically farthest nodes from the current source node 
    for t_node in farthest_target:
        output_str=""
        w_path_length = len(w_path[t_node]) - 1 #no of edges in weighted shortest path 
        if w_path_length >= 2:
                op_path_score = w_length[t_node]
                op_path = w_path[t_node]
                norm_path_score = op_path_score/(len(op_path)-1)
                cut_value, partition = nx.algorithms.flow.minimum_cut(Gcapacity, cur_node, t_node, capacity='weight')
                
                # min cut edges and constituent nodes                
                reachable, non_reachable = partition                 
                cutset = set()
                for u, nbrs in ((n, Gcapacity[n]) for n in reachable):  
                    cutset.update((u,v) for v in nbrs if v in non_reachable)
                min_cut_eds = sorted(cutset)               
                min_cut_nodes = []
                for e in min_cut_eds:
                    for n in e:
                        if n not in min_cut_nodes:
                            min_cut_nodes.append(n)
                
                output_str = str(cur_node)+'_'+ str(t_node)+'\t'+str(round(cut_value,5))+'\t'+str(round(op_path_score,5))+'\t'+str(round(norm_path_score,5))+'\t'+",".join(op_path)+'\t'+str(farthest_hops)+'\t'+str(min_cut_eds)+'\t'+",".join(min_cut_nodes)+'\n'
                final_op_strs.append(output_str)            
        else:
                pass

    return final_op_strs
                    
if __name__=="__main__":
        
    parser=argparse.ArgumentParser(description="This code takes in a capacity, inverse-capacity and network-edgelist file")
    parser.add_argument('capacityfile', help="a weighted (capacity) network file. Format: node1<tab>node2<tab>capacity")
    parser.add_argument('invcapacityfile', help="a weighted (inverse-capacity) network file. Format: node1<tab>node2<tab>inv_capacity")
    parser.add_argument('edgelistfile', help="an edgelist file for the network")
    parser.add_argument('opfile', help="name of output file for maxflow output")
    
    args=parser.parse_args()
    capfile=args.capacityfile
    invcapfile=args.invcapacityfile
    elistfile=args.edgelistfile
    maxflow_op_file=args.opfile

    Gcapacity = nx.read_weighted_edgelist(capfile, nodetype = str, create_using = nx.DiGraph())    
    Gicapacity = nx.read_weighted_edgelist(invcapfile, nodetype = str, create_using = nx.DiGraph())
    Gunw = nx.read_edgelist(elistfile, nodetype = str, create_using = nx.DiGraph())
    nodelist=list(Gcapacity.nodes())
    print('Total Nodes in Network:\t' + str(len(nodelist)))

    #writing output 
    header_line="Nodepair\tMaxflow_Score\tPath_Score\tNorm_Path_Score\tPath\tst_hops\tmincut_edges\tmincut_nodes\n"
    output_file = open(maxflow_op_file, "w")
    output_file.write(header_line) 
    #output in slices 
    s_slice = 30
    sliced_nodes = [nodelist[n:n+s_slice] for n in range(0,len(nodelist),s_slice)]
    #starmap to run func in parallel
    #np = mp.cpu_count() #using all available cpus 
    np = 4 #set no of processes 4 as default 
    pool = mp.Pool(np) 
    for cur_slice in sliced_nodes:
        op = pool.starmap(compute_op_scores, [(cur_node, Gcapacity, Gicapacity, Gunw) for cur_node in cur_slice])
        op_write = list(chain.from_iterable(op))
   #     print(op)
   #     print('\n')
   #     print(op_write)
   #     print('done with a slice'+str(cur_slice)+'\n')
        output_file.write("".join(str(i) for i in op_write))
    pool.close()
    output_file.close()
    print('Completed generating maxflowfile for given inputs\n')
    
