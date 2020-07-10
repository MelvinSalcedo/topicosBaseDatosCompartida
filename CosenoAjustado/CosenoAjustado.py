import multiprocessing 
import time
from math import sqrt
import operator
import pickle
import math

def CargarBinario( name ):
	with open('pkl_files/' + name + '.pkl', 'rb') as f:
		return pickle.load(f)
	
userRatings = CargarBinario("movilens20k_data")
#userRatings = CargarBinario("movilens27k_data")
#userRatings = CargarBinario("Anime_data")
userRatdings= {
"David": {"Imagine Dragons": 3, "Daft Punk": 5,"Lorde": 4, "Fall Out Boy": 1},
"Matt": {"Imagine Dragons": 3, "Daft Punk": 4,"Lorde": 4, "Fall Out Boy": 1},
"Ben": {"Kacey Musgraves": 4, "Imagine Dragons": 3,"Lorde": 3, "Fall Out Boy": 1},
"Chris": {"Kacey Musgraves": 4, "Imagine Dragons": 4,"Daft Punk": 4, "Lorde": 3, "Fall Out Boy": 1},
"Tori": {"Kacey Musgraves": 5, "Imagine Dragons": 4,"Daft Punk": 5, "Fall Out Boy": 3}}

averages = {}
print("data cargada")
def calculateM(user1,band1):
	tiempo0=time.time()
	longitud=len(userRatings[user1])

	for (key, ratings) in userRatings.items():
		averages[key] = (float(sum(ratings.values()))
						 /len(ratings.values()))
		
def multiprocessing_func(x,band1,data,return_dict):
	matriz={}
	similitudMatrz=[]
	for (band2, rr) in data.items():
		num = 0 # numerator
		dem1 = 0 # first half of denominator
		dem2 = 0
		if(band1!=band2):
			for (user, ratings) in userRatings.items():
				if band1 in ratings and band2 in ratings:
					avg = averages[user]
					num  += (ratings[band1] - avg)*(ratings[band2] - avg)
					dem1 += (ratings[band1] - avg)**2
					dem2 += (ratings[band2] - avg)**2
			cen=(sqrt(dem1) * sqrt(dem2))
			if(cen==0):
				result=0
			else:
				result=( num / cen)
			matriz[band2] = result
	return_dict[x]=(matriz)

def AjusteCose():	   
	starttime = time.time()
	manager = multiprocessing.Manager()
	processes = []
	return_dict = manager.dict()     
	user1="125794"
	band1="2343"
	#118205,1859    8405,143       82418,4124   121535,4325    125794,2343
	#2.7606 82.77	2.7651 76.27  2.06 59.66 - memory
	calculateM(user1,band1)
	#$print(averages)
	tamConsulta= math.ceil(len(userRatings[user1])/12)
	contador=0
	#print(len(userRatings[user1]),tamConsulta)

	data={};
	data2=[]
	i=0
	for (band2,r) in userRatings[user1].items():    

		#print(contador)
		data[band2]=r
		contador+=1
		i+=1
		if(contador==tamConsulta or i==len(userRatings[user1])):
			#print(data)
			data2.append(data)
			contador=0
			data={};



	#print("->",data2)   
	for x in range(len(data2)):
		p = multiprocessing.Process(target=multiprocessing_func, args=(x,band1,data2[x],return_dict))
		processes.append(p)
		p.start()
		
	for process in processes:
		process.join()
	matriz={}
	for(i,y) in return_dict.items():
		for o,oo in y.items():
			matriz[o] = oo  
	#print(matriz)
		
	print('That took {} seconds'.format(time.time() - starttime))
	print("--------------")


	maxNumber=max(userRatings[user1].items(), key=operator.itemgetter(1))[1]
	minNumber=min(userRatings[user1].items(), key=operator.itemgetter(1))[1]
	
	#print("hallando maximos y minimos",maxNumber,"--",minNumber)
	#print("max number is = ",maxNumber,minNumber);
	if(maxNumber==minNumber):
		print("No se puede dividir entre 0")
		return "No se puede dividir entre 0 , max=min"

	normalizeData={}
	for i,x in userRatings[user1].items():
		normalizeData[i]=(2*(x-minNumber)-(maxNumber-minNumber))/(maxNumber-minNumber)
	#print("data normalizada")
	#print(normalizeData)    
	numerador2=0
	denominador2=0
	#Predecir el puntaje que X dará a Y
	for i,x in normalizeData.items():
		if(i!=band1):
			numerador2+=matriz[i]*normalizeData[i]
			denominador2+=abs(matriz[i])
		#print(i,x)
	#print("num/den= ",numerador2,"/",denominador2,"=");    
	if(denominador2==0):
		print("No se puede dividir entre 0")
		return "No se puede dividir entre 0"
	p=numerador2/denominador2
	#desnormalizar
	resultadorFinal=0.5*((p+1)*(maxNumber-minNumber))+minNumber;
	print("RESULTADO FINAL ENTRE ",user1," Y ",band1 ,"=\t",resultadorFinal
		  ,"\t Tiempo = ",time.time()-starttime)
	return resultadorFinal

	print('That took {} seconds'.format(time.time() - starttime))




if __name__ == '__main__':
	AjusteCose()