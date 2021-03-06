import pandas as pd
import numpy as np
import argparse

parser = argparse.ArgumentParser(description="Multiply male genotype dosage by 2")

parser.add_argument(-i, --inputfile,"filename",type=str,help = "input file name")
parser.add_argument(-0, --outputfile,"outputname",type=str,help = "output file name")
args.parser.parse_args()
filename = "your_file_name.vcf"
outputname= "your_output_filename.vcf"
data = pd.read_csv(args.filename,sep='\t')

for n, d in data.iteritems():
    if n.startswith('IPC'):
        new_list = []
        for i in d:
            print(i)
            i = i.split(":")
            i[1] = float(i[1])
            i[1] = str(i[1]*2)
            new_list.append(i) 
        new_list = [':'.join(j) for j in new_list]
        data = data.drop(n, axis=1)
        data[n] = new_list

data.to_csv(args.outputname, sep='\t',index = False)
