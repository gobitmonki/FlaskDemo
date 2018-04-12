import os

count = 0
Files = []

def listFiles(dirpath):
    files = os.listdir(dirpath)
    for name in files:
        fullname = os.path.join(dirpath,name)
        Files.append(fullname)



listFiles('./data')
path = "./data/2.csv"
f = open(path)
for line in f.readlines():
    print line
