import os, sys
from itertools import chain, combinations
from collections import defaultdict


def readFile(fileName):
    """
    Read data from file
    :param fileName:
    :return:
    """
    with open(fileName) as inFile:
        for line in inFile:
            line = line.strip('\n\r')
            if line:
                yield line.split()


def generateCandidates(frequentSet, itemGroupSize):
    """
    Generate candidate set of size itemGroupSize from the previously frequent itemsets
    :param frequentSet:
    :param itemGroupSize:
    :return: candidateSet
    """
    print 'Generating Candidates for itemGroupSize - %s' % (itemGroupSize)
    if itemGroupSize == 1:
        return frequentSet

    result = []
    expandedItems = set([item for tup in frequentSet for item in tup])

    itemCombinations = chain(*[combinations(expandedItems, itemGroupSize)])
    for itemCombination in itemCombinations:
        subsetCombinations = chain(*[combinations([item for item in itemCombination], itemGroupSize - 1)])
        allSubsetsFrequent = True
        for subset in subsetCombinations:
            if tuple(sorted(subset)) not in frequentSet:
                allSubsetsFrequent = False
        if allSubsetsFrequent:
            result.append(tuple(sorted(itemCombination)))
    return result


def getFrequency(candidateSet, transactionList, itemGroupSize):
    print 'Fetching frequency for candidate set of size - %s' % (len(candidateSet))
    itemsetFrequency = defaultdict(int)
    for transaction in transactionList:
        itemCombinations = chain(*[combinations(transaction, itemGroupSize)])
        for itemCombination in itemCombinations:
            if tuple(sorted(itemCombination)) in candidateSet:
                itemsetFrequency[tuple(sorted(itemCombination))] += 1
    return itemsetFrequency


def generateFrequentSet(candidateSetFrequency, minSupport):
    print 'Generating Frequent Item Sets\n\n'
    result = {}
    for item, frequency in candidateSetFrequency.iteritems():
        if frequency >= minSupport:
            result[tuple(sorted(item))] = frequency
    return result


if __name__ == '__main__':
    dataFile = os.path.relpath('../resources/test1.txt')
    minSupport = 2
    frequentItemSets = {}
    transactionList = []
    frequentSet = set()

    for transaction in readFile(dataFile):
        transactionList.append(transaction)
        for item in transaction:
            frequentSet.add(item)

    itemGroupSize = 1
    stoppingCondition = False

    frequentSet = [itemset for itemset in chain(*[combinations(frequentSet, 1)])]

    while not stoppingCondition:
        candidateSet = generateCandidates(frequentSet, itemGroupSize)
        candidateSetFrequency = getFrequency(candidateSet, transactionList, itemGroupSize)
        frequentSet = generateFrequentSet(candidateSetFrequency, minSupport)
        if not frequentSet:
            stoppingCondition = True
        else:
            frequentItemSets[itemGroupSize] = frequentSet
            frequentSet = set(frequentSet.keys())
        itemGroupSize += 1

    print frequentItemSets



