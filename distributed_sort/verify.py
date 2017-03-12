import sys
output = list(int(e) for e in open(sys.argv[1]).read().split())
numbers = list(int(e) for e in open(sys.argv[2]).read().split())
print len(output)
print len(numbers)
print output == numbers
