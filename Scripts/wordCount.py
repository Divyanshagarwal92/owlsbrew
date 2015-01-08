import operator
from collections import Counter
from nltk.corpus import stopwords

inputFile = raw_input('Enter Input file: ')
outputFile = raw_input('Enter Output file: ')
inf = open(inputFile,'r')
sentence = inf.read()
sentence = sentence.replace('\n',' ')

f = open(outputFile,'w')
# word frequency
newsentence = []
stop = set(stopwords.words('english'))
stop.add('theowlsbrew')
for word in sentence.split():
	word = word.lower()
	if word not in stop:
		newsentence.append(word)

counts = Counter(newsentence)
sorted_counts = sorted(counts.items(), key=operator.itemgetter(1))
print sorted_counts
for tup in sorted_counts:
	if len(tup[0])>3 and tup[1]>=5:
		f.write(tup[0] + ' ' + str(tup[1]))
		f.write('\n')
		print tup[0] + ' ' +  str(tup[1])
f.close()
print '\n\n\n\n'

