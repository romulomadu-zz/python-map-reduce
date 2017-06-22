print 'HD - Homework 1 - Response'
print '\nA)'

for line in open('RESULT.csv'):
	if not line.startswith('Grzegorz Rozenberg'):
		continue
	lst = line.split(',')
	print 'Two more frequent words for',lst[0] + ':'
	print lst[1]
	print lst[2]	

print '\nB)'

for line in open('RESULT.csv'):
	if not line.startswith('Philip S. Yu'):
		continue
	lst = line.split(',')
	print 'Two more frequent words for',lst[0] + ':'
	print lst[1]
	print lst[2]