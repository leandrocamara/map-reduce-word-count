# -*- coding: utf-8 -*-
import mincemeat
import csv
import glob

path = '/home/leandro/Documents/puc/06-solutions/activities/map-reduce-word-count/'
text_files = glob.glob(path + 'data/*')

# Retorna o conteúdo do arquivo.
def file_contents (file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

# Lê cada linha do arquivo e transforma as palavras em uma estrutura "chave/valor".
def mapfn (k, v):
    print 'map ' + k
    from stopwords import allStopWords
    for line in v.splitlines():
        for word in line.split():
            if (word not in allStopWords):
                yield word, 1

# Soma a quantidade de ocorrências da palavra.
def reducefn (k, v):
    print 'reduce ' + k
    return sum(v)

# Transforma todos os arquivos em uma estrutura de "chave/valor" (file_name/file_content).
source = dict( (file_name, file_contents(file_name)) for file_name in text_files )

s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

# Apresenta o resultado em um arquivo CSV.
w = csv.writer( open(path + 'result.csv', 'w') )

for k, v in results.items():
    w.writerow([k, v])

# Name Node
# python word-count.py

# Data Nodes
# python mincemeat.py -p changeme localhost