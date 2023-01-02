import pandas as pd 
import argparse 
import numpy as np 
import csv 
import os 

if __name__=="__main__":

    parser = argparse.ArgumentParser(description="This script outputs ranked maxflow file based on percentile")
    parser.add_argument('maxflow_file', help="file to be ranked - headers in format of that generated by get_maxflow_files_parallel_farthest_t.py")
    parser.add_argument('input_percentile', help="Percentile of maxflow score, such that only flows with score greater than  this cut-off are retained")
    parser.add_argument('op_fname', help="output file path")

    args = parser.parse_args()
    max_file = args.maxflow_file
    per = float(args.input_percentile)
    op_file = args.op_fname
    op_temp = 'temp.tsv'

    mf_df = pd.read_csv(max_file, sep = "\t")
#    print(mf_df)
    ranked_mf_df = mf_df.sort_values(by = "Maxflow_Score", ascending = False)
    all_mscores = list(ranked_mf_df['Maxflow_Score'])
 
    print(len(all_mscores))
    
    p1 = np.percentile(all_mscores, per)

    header_line="Nodepair\tMaxflow_Score\tPath_Score\tNorm_Path_Score\tPath\tst_hops\tmincut_edges\tmincut_nodes\n"
    f1 = open(op_temp, "w")
    f1.write(header_line)
    for index, row in ranked_mf_df.iterrows():
        if row['Maxflow_Score'] >= p1:
            f1.write(str(row['Nodepair'])+'\t'+str(row['Maxflow_Score'])+'\t'+str(row['Path_Score'])+'\t'+str(row['Norm_Path_Score'])+'\t'+str(row['Path'])+'\t'+str(row['st_hops'])+'\t'+str(row['mincut_edges'])+'\t'+str(row['mincut_nodes'])+'\n')
    f1.close()
    
    p1_nm_sort_df = pd.read_csv(op_temp, sep = '\t')
    ranked_p1_nm_df = p1_nm_sort_df.sort_values(by = ['Norm_Path_Score'], ascending = True)
    ranked_p1_nm_df.to_csv(op_file,index = False, sep = '\t', quoting=csv.QUOTE_NONE,header=True)
    os.remove(op_temp)
    print('generated ranked files for'+'\t'+str(per)+'\t'+'percentile'+'\n')