import sys


def generateStats(filePath):
    maxSize = 0
    minSize = 99999999
    numTransactions = 0
    allTransactionLength = 0
    avgTransactionLength = 0
    with open(filePath) as inFile:
        for line in inFile:
            line = line.strip('\n\r')
            if line:
                numTransactions += 1
                lineSplit = line.strip().split()
                allTransactionLength += len(lineSplit)
                if len(lineSplit) > maxSize:
                    maxSize = len(lineSplit)
                if len(lineSplit) < minSize:
                    minSize = len(lineSplit)
        avgTransactionLength = (allTransactionLength * 1.0) / numTransactions

    print 'Total Transactions : ' + str(numTransactions)
    print 'Max. Transaction Length : ' + str(maxSize)
    print 'Min. Transaction Length : ' + str(minSize)
    print 'Avg. Transaction Length : ' + str(avgTransactionLength)


if __name__ == '__main__':
    generateStats('/Users/KonceptGeek/Downloads/testing data/testing2.txt')