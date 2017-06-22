import mincemeat
import glob
import csv
import re
import string
import os

text_files = glob.glob(".\\Trab2.3\\*")

#==============================================================================
# Function to read file contents
#==============================================================================
def file_contents(file_name):
    f = open(file_name, 'r')
    try:
        return f.read()
    finally:
        f.close()
#==============================================================================
# Functions Map and Reduce for the first processing loop
#==============================================================================
def mapfn1(k,v):
    print 'map ' + k 
    from re import findall,sub
    for line in v.splitlines():
        lstAuthor = findall(':::(.+):::',line)[0].split('::') #get authors
        title = findall(':::.+:::(.+)',line)[0] #get title
        title = sub(r'[\.:,!()<>]', ' ', title) #removing punctuation
        title = sub(r'\d+', '', title).strip().lower() #removing numbers, lowercase
        for author in lstAuthor:
            yield author, title #returns (key, value) pair as (author, title)

def reducefn1(k,v):
    print 'reduce ' + k 
    return ' '.join(v) #join all titles related with some author

#==============================================================================
# Functions Map and Reduce for the second processing loop
#==============================================================================
def mapfn2(k,v):
    print 'map ' + k
    from stopwords import allStopWords
    for word in v.split():
        if word in allStopWords:
            continue
        if len(word)>1:
            yield k+'::'+word, 1 #returns key, value pair as (author::word,1)

def reducefn2(k,v):
    print 'reduce ' + k 
    return sum(v) #count frequency of some author::word

#==============================================================================
# Functions Map and Reduce for the third processing loop
#==============================================================================
def mapfn3(k,v):
    print 'map ' + k
    author = k.split('::')[0]
    word =  k.split('::')[1]
    yield author,(word,v) #separate words and its count by author and returns (author, (word, count))

def reducefn3(k,v):
    print 'reduce ' + k 
    L = sorted(v, key=lambda x: x[1])
    L.reverse()
    return L #returns a list of counted words by author

#==============================================================================
# Run first MapReduce process           
#==============================================================================
print 'Loading initial data.'
source = dict((file_name, file_contents(file_name)) for file_name in text_files)
print 'Finished.'

s = mincemeat.Server()

s.datasource = source

s.mapfn = mapfn1

s.reducefn = reducefn1

print 'Executing first Map/Reduce process.'
results = s.run_server(password = "changeme")
print 'Finished.'

print 'Partial result saved in RESULT1.csv'
w = csv.writer(open("RESULT1.csv","w"))

for k, v in results.items():
    w.writerow([k,v])

#==============================================================================
# Run second loop MapReduce            
#==============================================================================
s.datasource = results

s.mapfn = mapfn2

s.reducefn = reducefn2

print 'Executing second Map/Reduce process.'
results = s.run_server(password = "changeme")
print 'Finished.'

print 'Partial result saved in RESULT2.csv'    
w = csv.writer(open("RESULT2.csv","w"))

for k, v in results.items():
    w.writerow([k,v])

#==============================================================================
# Run third loop MapReduce            
#==============================================================================
s.datasource = results

s.mapfn = mapfn3

s.reducefn = reducefn3

print 'Executing third Map/Reduce process.'
results = s.run_server(password = "changeme")
print 'Finished.'

#==============================================================================
# Write results in .csv file
#==============================================================================
w = open("RESULT.csv","w")

for k, v in results.items():
    s = k    
    for key, value in v:
        s += ', ' + key + ': ' + str(value)
    w.write(s + '\n')

w.close()    

print 'Result saved on RESULT.csv'
print 'Exit.'
    

