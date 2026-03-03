import time

for i in range(20,0,-1):
	print("\033c",end='')
	print(i)
	print(i-1)
	time.sleep(1)