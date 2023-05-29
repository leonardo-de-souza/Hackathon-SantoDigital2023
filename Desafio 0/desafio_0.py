import math, sys

def exercicio1(n):
    lista = []

    while n > 0:
        str = ""

        for i in range(n):
            str += "*"

        lista.append(str)
        n -= 1

    lista.reverse()
    return lista

def exercicio2(array):
    array = sorted(array)
    min_difference = sys.maxsize
    pairs = []

    for i in range(len(array) - 1):
        difference = abs(array[i+1] - array[i])

        if difference < min_difference:
            min_difference = difference
            pairs = [(array[i], array[i+1])]
        
        elif difference == min_difference:
            pairs.append((array[i], array[i+1]))

    return pairs

def exercicio3(array):
    # O subconjunto vazio sempre está incluido
    subsets = [[]]  

    for n in array:
        possible_subsets = len(subsets)

        for i in range(possible_subsets):
            new_subset = subsets[i].copy()
            new_subset.append(n)
            subsets.append(new_subset)

    return subsets
