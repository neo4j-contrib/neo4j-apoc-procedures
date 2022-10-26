# Python3 program to convert
# list into a list of lists

def extractDigits(lst):
	res = []
	for el in lst:
		sub = el.split(', ')
		res.append(sub)
	
	return(res)
				
# Driver code
lst = ['alice', 'bob', 'cara']
print(extractDigits(lst))
