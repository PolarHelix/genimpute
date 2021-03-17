import pandas as pd
import numpy as np
import argparse

parser = argparse.ArgumentParser(description="Get DS from GT")

parser.add_argument("-i",type=str,help = "input file name")
parser.add_argument("-o",type=str,help = "output file name")
args = parser.parse_args()

data = pd.read_csv(str(args.i),sep='\t',comment="#",compression='gzip')
data.iloc[:,8] = "GT:DS"

for n, d in data.iloc[:,9:].iteritems():
    
        new_list = []
        for i in d:
            i1 = i.split("|")
            ds = int(i1[0]) + int(i1[1])
            new_list.append(i+":"+str(ds)) 
        data = data.drop(n, axis=1)
        data[n] = new_list

data.to_csv(str(args.o), sep='\t',index = False)



