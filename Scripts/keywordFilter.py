# Cocktail keywords from comments
from alchemyapi import AlchemyAPI
alchemyapi = AlchemyAPI()

inputFile = raw_input('Enter Input file: ')
outputFile = raw_input('Enter Output file: ')
inf = open(inputFile,'r')
sentence = inf.read()
sentence = sentence.replace('\n',' ')
f = open(outputFile,'w')
response = alchemyapi.keywords('text', sentence, {'keywordExtractMode':'strict', 'maxRetrieve': 100, 'knowledgeGraph': 1} )
graph = []
for keyword in response['keywords']:
	phrase = keyword['text']
	#if phrase.find('tea') != -1: 
	if phrase.find('cocktail') != -1: 
		f.write(phrase)
		f.write(' ')
		f.write(str(keyword['relevance']))
		f.write('\n')
		print phrase

