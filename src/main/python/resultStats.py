import os, sys
from collections import defaultdict

def getStats(fileName):
    size = defaultdict(int)
    totalItemsets = 0
    with open(fileName) as inFile:
        for line in inFile:
            line = line.strip('\n\r')
            if line:
                lineSplit = line.split('\t')
                if len(lineSplit) == 1:
                    totalItemsets = int(lineSplit[0])
                items = lineSplit[0].split()
                size[len(items)] += 1

    print '########################################################'
    print 'Stats for file: ' + fileName
    print 'Total Frequent Itemsets: ' + str(totalItemsets)
    print 'Number of frequent itemsets with respect to their size: '
    for key, value in size.iteritems():
        print str(key) + '\t' + str(value)
    print '########################################################'


if __name__ == '__main__':

    for file in os.listdir(sys.argv[1]):
        if file.endswith('_result.dat'):
            getStats(sys.argv[1]+file)

