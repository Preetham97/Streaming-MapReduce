
##########################################################################
## Simulator.py  v 0.1
##
## Implements two versions of a multi-level sampler:
##
## 1) Traditional 3 step process
## 2) Streaming process using hashing
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
## Spring 2020
##
## Student Name: Preetham Akhil Bhuma
## Student ID: 112584830

##Data Science Imports: 
import numpy as np
import mmh3
import random

import time


##IO, Process Imports: 
import sys
from pprint import pprint


##########################################################################
##########################################################################
# Typical non-streaming multi-level sampler

def typicalSampler(filename, percent = .01, sample_col = 0):
    # Implements the standard non-streaming sampling method
    # Step 1: read file to pull out unique user_ids from file
    # Step 2: subset to random  1% of user_ids
    # Step 3: read file again to pull out records from the 1% user_id and compute mean withdrawn
    
    

    mean, standard_deviation = 0.0, 0.0
    
    
    ##<<COMPLETE>>
    uniqueset = set()
    subset = set()
    
      
    #step 1
   
    for line in filename: 
        
        x = line.split(',')
        uniqueset.add(x[sample_col])
    
        
    #step 2: 
    
    uniqueList = list(uniqueset)
    random.shuffle(uniqueList)
    #print(uniqueList)
    
    
    subset = uniqueList[:round(len(uniqueList)*percent)]
    
    
    #step 3:
    filename.seek(0)
    
    count =0
    mean =0
    M2 =0
    
    for line in filename:
          
        x= line.split(',')
        
        if(x[sample_col] in subset):
            count+=1
            var = float(x[-1])
            delta = var - mean
            mean += delta/count
            delta2 = var - mean
            M2+= delta*delta2
    
        

    #mean = sumy/ count
    standard_deviation = np.sqrt(M2/count)
    
    
    return mean, standard_deviation


##########################################################################
##########################################################################
# Streaming multi-level sampler

def streamSampler(stream, percent = .01, sample_col = 0):
    # Implements the standard streaming sampling method:
    #   stream -- iosteam object (i.e. an open file for reading)
    #   percent -- percent of sample to keep
    #   sample_col -- column number to sample over
    #
    # Rules:
    #   1) No saving rows, or user_ids outside the scope of the while loop.
    #   2) No other loops besides the while listed. 
    
    mean, standard_deviation = 0.0, 0.0
    ##<<COMPLETE>>
    count =0
    mean =0
    M2 =0
    
    num_buckets = 1/percent
    for line in stream:
        ##<<COMPLETE>>
        x= line.split(',')
        if(mmh3.hash(x[sample_col])%num_buckets == 0):
            count+=1
            var = float(x[-1])
            delta = var - mean
            mean += delta/count
            delta2 = var - mean
            M2+= delta*delta2 
            
    
    standard_deviation = np.sqrt(M2/count)
            
    pass
    ##<<COMPLETE>>

    return mean, standard_deviation

##########################################################################
##########################################################################
# Timing

files=['transactions_small.csv', 'transactions_medium.csv', 'transactions_large.csv']
percents=[.02, .005]

if __name__ == "__main__": 

    ##<<COMPLETE: EDIT AND ADD TO IT>>
    for perc in percents:
        print("\nPercentage: %.4f\n==================" % perc)
        for f in files:
            print("\nFile: ", f)
            fstream = open(f, "r")
            start = time.process_time ()
            print("  Typical Sampler: ", typicalSampler(fstream, perc, 2))
            end = time.process_time ()
            print("Processing time:", end - start)
            fstream.close()
            fstream = open(f, "r")
            start = time.process_time ()
            print("  Stream Sampler:  ", streamSampler(fstream, perc, 2))
            end = time.process_time ()
            print("Processing time:", end - start)
            







