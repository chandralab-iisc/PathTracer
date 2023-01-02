#Script to output edgeweight files for capacities and inverse-capacities (and plain edge weight file) 
import pandas as  pd 
import numpy as np
import argparse 
import csv 

def get_edgeweights(df_network,df_fc):
    #Considering only those edges where both nodes of that edge are present in the fold-change dataframe 
    subnet = df_network[(df_network['node1'].isin(df_fc['gene'])) & (df_network['node2'].isin(df_fc['gene']))]
    subnet.reset_index(drop=True,inplace=True)
#    subnet.is_copy = False 
    #Getting all col names from FC table 
    subnet_copy = subnet.copy()
    cols=[]
    for i in df_fc.columns:
            cols.append(i)
    cols.remove('gene')
    print(cols)
    nodes1=list(subnet['node1'])
    nodes2=list(subnet['node2'])
    #Setting gene column in FC table as index for the dataframe df_fc
    df_fc.set_index("gene",inplace=True)    
    #Note: Followed earlier scripts and added 0.0001 as a sort of pseudocount for the edgeweights
    df_fc = df_fc + 0.0001
    #Locating nodes from node1 and node2 columns in df_fc
    nodeA=df_fc.loc[nodes1]
    nodeB=df_fc.loc[nodes2]
    #Reset index for nodeA and nodeB 
    nodeA.reset_index(drop=True,inplace=True)
    nodeB.reset_index(drop=True,inplace=True)
    
    for i in cols:
        cap = np.sqrt(nodeA[i]*nodeB[i]) 
        subnet_copy['inv_capacity'] = 1/cap
        subnet_copy['capacity'] = cap
        op_file_ew='./'+ i +'_inv_cap.csv'
        op_file_cap='./'+ i +'_capacity.csv'
        op_file_elist='./'+ i + '_unweighted_elist.csv'
        inv_cap_cols=['node1','node2','inv_capacity']
        cap_cols=['node1','node2','capacity']
        elist_cols=['node1','node2']
        subnet_copy.to_csv(op_file_ew,columns=inv_cap_cols,index=False,sep='\t', quoting=csv.QUOTE_NONE, header=False)
        subnet_copy.to_csv(op_file_cap,columns=cap_cols,index=False,sep='\t', quoting=csv.QUOTE_NONE,header=False)
        subnet_copy.to_csv(op_file_elist, columns=elist_cols, index=False,sep='\t', quoting=csv.QUOTE_NONE,header=False)
#        print(subnet.head())
        print(i)
     
if __name__ == "__main__":
    
    parser=argparse.ArgumentParser(description="This code takes in a network_file and a nodeweights_file and outputs capacity, ")
    parser.add_argument('networkfile', help="Please provide network file. Format: node1<tab>node2" )
    parser.add_argument('nodeweightsfile', help='Please provide nodeweights file. Format: gene<tab>FoldChange ;Header line  should be "gene" followed by tab separated column-names of weights columns')
    
    args = parser.parse_args()
    netfile = args.networkfile
    nodefile = args.nodeweightsfile
    #read network into df_network and foldchange into df_fc
    df_network=pd.read_csv(netfile,sep='\t')
    df_fc=pd.read_csv(nodefile,sep='\t')
    df_network.columns=['node1','node2'] 
    get_edgeweights(df_network, df_fc)
    
    
    
    
    
    
