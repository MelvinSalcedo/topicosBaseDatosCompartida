import codecs 
import time
import math
from math import sqrt
import pickle
import time
import numpy as np
import multiprocessing
import os
from multiprocessing import Queue
from multiprocessing import Pool
from multiprocessing import Process, Manager
import itertools
import pandas as pd

users = {"Angelica": {"Blues Traveler": 3.5, "Broken Bells": 2.0, "Norah Jones": 4.5, "Phoenix": 5.0,
"Slightly Stoopid": 1.5,
"The Strokes": 2.5, "Vampire Weekend": 2.0},
"Bill": {"Blues Traveler": 2.0, "Broken Bells": 3.5,
"Deadmau5": 4.0, "Phoenix": 2.0,
"Slightly Stoopid": 3.5, "Vampire Weekend": 3.0},
"Chan": {"Blues Traveler": 5.0, "Broken Bells": 1.0,
"Deadmau5": 1.0, "Norah Jones": 3.0,
"Phoenix": 5, "Slightly Stoopid": 1.0},
"Dan": {"Blues Traveler": 3.0, "Broken Bells": 4.0,
"Deadmau5": 4.5, "Phoenix": 3.0,
"Slightly Stoopid": 4.5, "The Strokes": 4.0,
"Vampire Weekend": 2.0},
"Hailey": {"Broken Bells": 4.0, "Deadmau5": 1.0,
"Norah Jones": 4.0, "The Strokes": 4.0,
"Vampire Weekend": 1.0},
"Jordyn": {"Broken Bells": 4.5, "Deadmau5": 4.0, "Norah Jones": 5.0,
"Phoenix": 5.0, "Slightly Stoopid": 4.5,
"The Strokes": 4.0, "Vampire Weekend": 4.0},
"Sam": {"Blues Traveler": 5.0, "Broken Bells": 2.0,
"Norah Jones": 3.0, "Phoenix": 5.0,
"Slightly Stoopid": 4.0, "The Strokes": 5.0},
"Veronica": {"Blues Traveler": 3.0, "Norah Jones": 5.0,
"Phoenix": 4.0, "Slightly Stoopid": 2.5,
"The Strokes": 3.0}}


#Similitud cose multi_processing

def getPromedio(ratingsUser,item,item2):
    if item in ratingsUser and item2 in ratingsUser:  
        promedio=0
        denominador=1
        ratings={
            item : [],
            item2 :[]
        }
        for key in ratingsUser:
            promedio+=ratingsUser[key]
            if(key==item):
                ratings[item].append(ratingsUser[key])
            if(key==item2):
                 ratings[item2].append(ratingsUser[key])
        promedio=promedio/len(ratingsUser)
        numerador = (promedio-ratingsUser[item])*(promedio-ratingsUser[item2])
        return (ratings,numerador,promedio)
    return False




def similitudCoseno_mp(ratingsUser, ratingsUser2):
    xoy = 0
    normax = 0
    normay = 0
    for key in ratingsUser[1]:
        normax+=pow(ratingsUser[1][key],2)
        if key in ratingsUser2[1]:
            xoy+=ratingsUser[1][key]*ratingsUser2[1][key]
    for key in ratingsUser2[1]:
        normay+=pow(ratingsUser2[1][key],2)
    normax = sqrt(normax)
    normay = sqrt(normay)
    if normax*normay==0: 
        return (ratingsUser2[0],0)

    return (ratingsUser2[0],xoy/(normax*normay))


def pearson_mp(ratingsUser, ratingsUser2):
    sum_xy = 0
    sum_x = 0
    sum_y = 0
    sum_x2 = 0
    sum_y2 = 0
    n = 0
    for key in ratingsUser[1]:
        if key in ratingsUser2[1]:
            n += 1
            x = ratingsUser[1][key]
            y = ratingsUser2[1][key]
            sum_xy += x * y
            sum_x += x
            sum_y += y
            sum_x2 += pow(x, 2)
            sum_y2 += pow(y, 2)
    if n == 0:
        return (ratingsUser2[0], 0)
    denominator = (sqrt(sum_x2-pow(sum_x, 2)/n) * sqrt(sum_y2-pow(sum_y, 2)/n))
    if denominator == 0:
        distance = 0
    else:
        distance = (sum_xy - (sum_x * sum_y) / n) / denominator
    return (ratingsUser2[0], distance)

def distanciaManhattan_mp(ratingsUser, ratingsUser2):
    distancia = 0
    enComun = False
    for key in ratingsUser[1]:
        if key not in ratingsUser[1]: 
            continue
        if key in ratingsUser2[1]:
            distancia += abs(ratingsUser[1][key] - ratingsUser2[1][key])
    return (ratingsUser2[0], distancia)


def distanciaEuclidiana_mp(ratingsUser, ratingsUser2):
    distancia = 0
    for key in ratingsUser[1]:
        if key not in ratingsUser[1]: 
            continue
        if key in ratingsUser2[1]:
            distancia += math.pow((ratingsUser[1][key] - ratingsUser2[1][key]),2)
    return (ratingsUser2[0], math.sqrt(distancia))

'''
def distanciaMinkowski(usuario1, usuario2, r):
    distancia = 0
    for key in usuario1:
        if key not in usuario1: 
            continue
        if key in usuario2:
            distancia += math.pow(abs(usuario1[key] - usuario2[key]),r)
    return math.pow(distancia, 1/r)
'''

class Recomendador:

    def __init__(self, data, k=1, metric='pearson', n=5):
        self.k = k
        self.n = n
        self.username2id = {}
        self.userid2name = {}
        self.productid2name = {}
        self.metric = metric

        if type(data).__name__ == 'dict':
            self.data = data

    def loadMovieRatingsDB(self, path=''):
        self.data = {}
        self.data2 = pd.io.parsers.read_csv(path,sep=",",index_col=0, skip_blank_lines =True)

        for key in self.data2: #loop trought users
            newRatings = {}
            for k,v in self.data2[key].iteritems():
                if not pd.isnull(v):
                    newRatings[k] = v
            self.data[key] = newRatings
                
    def convertProductID2name(self, id):
        if id in self.productid2name:
            return self.productid2name[id]
        else:
            return id

    def loadBookDB(self, path=''):
        '''
        manager = Manager() 
        self.data = manager.dict()
       
        self.data = {}
        f = codecs.open(path + "BX-Book-Ratings.csv", 'r', 'utf8')
        for line in f:
            fields = line.split(';')
            user = fields[0].strip('"')
            book = fields[1].strip('"')
            rating = int(fields[2].strip().strip('"'))
            if user in self.data:
                currentRatings = self.data[user]
            else:
                currentRatings = {}
            currentRatings[book] = rating
            self.data[user] = currentRatings
        f.close()
        self.save_obj(self.data, "ratings_books")
        self.data = self.load_obj("ratings_books")
        
        '''
        self.data = self.load_obj("ratings_books")
        #self.data.update(self.load_obj("ratings_books"))
        '''
        f = codecs.open(path + "BX-Books.csv", 'r', 'utf8')
        for line in f:
            fields = line.split(';')
            isbn = fields[0].strip('"')
            title = fields[1].strip('"')
            author = fields[2].strip().strip('"')
            title = title + ' by ' + author
            self.productid2name[isbn] = title
        f.close()
        self.save_obj(self.productid2name, "product_books") 
        '''
        self.productid2name = self.load_obj("product_books")
        
        
        f = codecs.open(path + "BX-Users.csv", 'r', 'utf8')
        for line in f:
            fields = line.split(';')
            userid = fields[0].strip('"')
            location = fields[1].strip('"')
            if len(fields) > 3:
                age = fields[2].strip().strip('"')
            else:
                age = 'NULL'
            if age != 'NULL':
                value = location + '  (age: ' + age + ')'
            else:
                value = location
            self.userid2name[userid] = value
            self.username2id[location] = userid
        f.close()

    #Load movielens

    def loadMovieLens(self, path=''):
        self.data = self.load_obj("ratings10m")
        self.productid2name = self.load_obj("product_movies10m")
        '''
        self.data = {}
        i = 0
        f = codecs.open(path + "ratings.dat", 'r', 'utf8')
        for line in f:
            i += 1
            fields = line.split('::')
            user = fields[0].strip('"')
            movie = fields[1].strip('"')
            rating = float(fields[2].strip().strip('"'))
            if user in self.data:
                currentRatings = self.data[user]
            else:
                currentRatings = {}
            currentRatings[movie] = rating
            self.data[user] = currentRatings
        f.close()

        self.save_obj(self.data, "ratings10m")

        f = codecs.open(path + "movies.dat", 'r', 'utf8')
        for line in f:
            i += 1
            fields = line.split('::')
            movieId = fields[0].strip('"')
            title = fields[1].strip('"')
            genre = fields[2].strip().strip('"')
            title = title + ', ' + genre
            self.productid2name[movieId] = title
        f.close()

        self.save_obj(self.productid2name, "product_movies10m")
        '''
    
    #Load movielens 20M
    def loadMovieLens20M(self, path=''):
        self.data = self.load_obj("ratings20m")
        #return
        '''
        f = codecs.open(path + "ratings.csv", 'r', 'utf8')
        for line in f:
            try:
                fields = line.split(',')
                user = fields[0].strip('"')
                movie = fields[1].strip('"')
                rating = float(fields[2].strip().strip('"'))
                if user in self.data:
                    currentRatings = self.data[user]
                else:
                    currentRatings = {}
                currentRatings[movie] = rating
                self.data[user] = currentRatings
            except Exception:
                continue

        self.save_obj(self.data, "ratings20m")
        f.close()
        '''
        '''
        f = codecs.open(path + "movies.csv", 'r', 'utf8')
        for line in f:
            fields = line.split(',')
            movieId = fields[0].strip('"')
            title = fields[1].strip('"')
            genre = fields[2].strip().strip('"')
            title = title + ', ' + genre
            self.productid2name[movieId] = title
        f.close()
        self.save_obj(self.productid2name, "product_movies20m")
        '''
        self.productid2name = self.load_obj("product_movies20m")

    #Load movielens 27M
    def loadMovieLens27M(self, path=''):
        self.data = self.load_obj("ratings27m")
        '''
        f = codecs.open(path + "ratings.csv", 'r', 'utf8')
        for line in f:
            try:
                fields = line.split(',')
                user = fields[0].strip('"')
                movie = fields[1].strip('"')
                rating = float(fields[2].strip().strip('"'))
                if user in self.data:
                    currentRatings = self.data[user]
                else:
                    currentRatings = {}
                currentRatings[movie] = rating
                self.data[user] = currentRatings
            except Exception:
                continue
        self.save_obj(self.data, "ratings27m")
        f.close()
        f = codecs.open(path + "movies.csv", 'r', 'utf8')
        for line in f:
            fields = line.split(',')
            movieId = fields[0].strip('"')
            title = fields[1].strip('"')
            genre = fields[2].strip().strip('"')
            title = title + ', ' + genre
            self.productid2name[movieId] = title
        f.close()
        self.save_obj(self.productid2name, "product_movies27m")
        '''
        self.productid2name = self.load_obj("product_movies27m")
        

    def distanciaMinkowski(self, usuario1, usuario2, r):
        distancia = 0
        for key in usuario1:
            if key not in usuario1: 
                continue
            if key in usuario2:
                distancia += math.pow(abs(usuario1[key] - usuario2[key]),r)
        return math.pow(distancia, 1/r)


    def cacularDistanciasKnn(self, username):
        i = 0
        usuarios = list(self.data.keys())
        tamTotal = len(usuarios)
        print("Total de usuarios:", tamTotal)
        self.username = username

        ratingsUser = [username,self.data[username]]
        dictlist = []
        dictlist = [ (ratingsUser, [k,v]) for k, v in self.data.items() if k != username]
        t = time.process_time()
        number_of_workers = 24
        with Pool(number_of_workers) as p:
            if self.metric == "pearson":
                distances = p.starmap(pearson_mp, dictlist)
            elif self.metric == "manhattan":
                distances = p.starmap(distanciaManhattan_mp, dictlist)
            elif self.metric == "euclidiana":
                distances = p.starmap(distanciaEuclidiana_mp, dictlist)
            elif self.metric == "coseno":
                distances = p.starmap(similitudCoseno_mp, dictlist)
            
        print("time to process: " , time.process_time()-t)
        if self.metric == "pearson":
            distances.sort(key=lambda artistTuple: artistTuple[1], reverse=True) # ordenar basado en la distancia, de menor a mayor
        elif self.metric == "manhattan":
            distances.sort(key=lambda artistTuple: artistTuple[1]) # ordenar basado en la distancia, de menor a mayor
        elif self.metric == "euclidiana":
            distances.sort(key=lambda artistTuple: artistTuple[1]) # ordenar basado en la distancia, de menor a mayor
        elif self.metric == "minkowski":
            distances.sort(key=lambda artistTuple: artistTuple[1]) # ordenar basado en la distancia, de menor a mayor
        elif self.metric == "coseno":
            distances.sort(key=lambda artistTuple: artistTuple[1], reverse=True) # ordenar basado en la distancia, de menor a mayor
        
        return distances

    def save_obj(self, obj, name ):
        with open('pkl_files/'+ name + '.pkl', 'wb') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

    def load_obj(self, name ):
        with open('pkl_files/' + name + '.pkl', 'rb') as f:
            return pickle.load(f)

    def recommend(self, user):
        userRatings = self.data[user] #obtener las calificaciones del usuario
        try: 
            nearest = self.cacularDistanciasKnn(user)  #obtener la lista de usuarios, ordenados desde mas cercano a mas lejano
            #print(nearest)
        except Exception as e:
            print(e)
            print("No se encontro el usuario en la Base de datos")
            return []

        sumaDistancias = 0.0
        for i in range(self.k):
            print("Vecino cercano:", nearest[i])
            sumaDistancias += nearest[i][1]
        if sumaDistancias == 0.0:
            print("No se encontraron recomendaciones")
            return []
        recommendations = {}
        for i in range(self.k):        # acumular las calificaciones de los k vecinos
            weight = nearest[i][1] / sumaDistancias  # calcular su porcentaje 
            name = nearest[i][0] # obtener el nombre de la persona

            if name==user:
                continue
            #print (name)
            neighborRatings = self.data[name] # obtener las calififaciones de esa persona
            #print(neighborRatings)
            #print("Vecino cercano:" + name)
            for artist in neighborRatings: #buscar las calificaciones que el vecino hizo y el usuario no hizo
                if not artist in userRatings:
                    if artist not in recommendations:
                        #    if neighborRatings[artist] > userRatings[artist]:
                        recommendations[artist] = neighborRatings[artist]
                    else:
                        #if neighborRatings[artist] > userRatings[artist]:
                        recommendations[artist] = (recommendations[artist] + neighborRatings[artist])/2
                    
        recommendations = list(recommendations.items())
        recommendations = [(self.convertProductID2name(k), v)
                          for (k, v) in recommendations]
        recommendations.sort(key=lambda artistTuple: artistTuple[1], reverse = True)
        return recommendations[:self.n] # devolver las recomendaciones solicitadas

    def porcentajeProyectado(self, user, item):
        userRatings = self.data[user] #obtener las calificaciones del usuario
        #print("Calificaciones del usuario:")
        #print(userRatings)
        try: 
            nearest = self.cacularDistanciasKnn(user)  #obtener la lista de usuarios, ordenados desde mas cercano a mas lejano
            #print(nearest)
        except Exception as e:
            print("No se encontro el usuario en la Base de datos")
            return []

        sumaDistancias = 0.0
        for i in range(self.k):
            if item not in self.data[nearest[i][0]]:  #el vecino no califico esa pelicula
                continue
            sumaDistancias += nearest[i][1]
        if sumaDistancias == 0.0:
           print("No se encontraron recomendaciones")
           return []
        recommendations = {}
        for i in range(self.k):        # acumular las calificaciones de los k vecinos
            weight = nearest[i][1] / sumaDistancias  # calcular su porcentaje 
            name = nearest[i][0] # obtener el nombre de la persona
            neighborRatings = self.data[name] # obtener las calififaciones de esa persona
            #print(neighborRatings)
            if item not in neighborRatings:#el vecino no califico esa pelicula
                continue
            print("Name:", name, ":", neighborRatings[item])
            #if item in neighborRatings and not pd.isnull(neighborRatings[item]):
            #print(neighborRatings)
            if item not in recommendations:
                recommendations[item] = neighborRatings[item] * weight
            else:
                recommendations[item] = (recommendations[item] + neighborRatings[item] * weight)
        print(recommendations)
        return recommendations[item]
    

    def cosenoAjustado(self,item1,item2):
        numerador=0
        ratings1=[]
        ratings2=[]
        for user in self.data:
            data=getPromedio(self.data[user],item1,item2)
            if(data!=False):
                numerador += data[1]
                ratings1.append(data[0][item1][0])
                ratings2.append(data[0][item2][0])
        print(ratings1)


if __name__ == '__main__':
    recomendador = Recomendador({}, k=4, metric='coseno', n=4)
    recomendador.loadMovieRatingsDB("test1.csv")
    recomendador.cosenoAjustado("Kacey","Imagine")

    '''
    recomendador.loadBookDB("dataset/BX-Dump/")
    print("time to load data books db: " , time.process_time()-t)
    t= time.process_time()
    resultado = recomendador.recommend("171118")
    #print("time process books db" , time.process_time()-t)
    '''
    '''
    t= time.process_time()
    recomendador.loadMovieLens20M("ml-20m/")
    print("time to load movielens 20M db: " , time.process_time()-t)
    t= time.process_time()
    resultado = recomendador.recommend("26894")
    #print("time to process movielens 20M db: " , time.process_time()-t)
    print(resultado)
    '''
    '''
    recomendador = Recomendador({}, k=10, metric='coseno', n=5)
    recomendador.loadMovieRatingsDB("dataset/")
    print(recomendador.porcentajeProyectado("Jessica", "Alien"))


    recomendador = Recomendador({}, k=8, metric='pearson', n=5)
    recomendador.loadMovieRatingsDB("dataset/")
    print(recomendador.porcentajeProyectado("Katherine", "Spiderman"))
    '''
    #recomendador = Recomendador({}, k=10, metric='coseno', numRecomendaciones=5)
    #recomendador.loadMovieRatingsDB("dataset/")
    #print(recomendador.porcentajeProyectado("Jessica", "Alien"))


    #recomendador = Recomendador({}, k=8, metric='pearson', numRecomendaciones=5)
    #recomendador.loadMovieRatingsDB("dataset/")
    #print(recomendador.porcentajeProyectado("Katherine", "Spiderman"))
    #print(recomendador.multipleRecommend(["Patrick C", "Matt"]))


'''
t= time.process_time()
recomendador.loadMovieLens("ml-10M100K/")
print("load data movielens 10M db: " , time.process_time()-t)
'''
'''
t= time.process_time()
recomendador.loadMovieLens20M("ml-20m/")
print("time to load movielens 20M db: " , time.process_time()-t)
t= time.process_time()
print(recomendador.recommend("26894"))
print("time to process movielens 20M db: " , time.process_time()-t)
'''
'''
recomendador.loadMovieLens27M("ml-latest/")
print("time to load movielens 27M db: ", time.process_time()-t)
t= time.process_time()
print(recomendador.recommend("26894"))
print("time to process movielens 27M db: ", time.process_time()-t)
'''
#26894
#138493


#print(recomendador.similitudCoseno(users["Angelica"], users["Veronica"]))

#Base de datos pequeña
#1 3knn hailey distancia coseno q' recomendaria
#print("-------------------1---------------------------")
#recomendador = Recomendador(users, k=8, metric='coseno', n=5)
#print("Mas cercanos:")
#print(recomendador.recommend("Hailey"))

#recomendador.data.to_csv("usuarios.csv", sep=',', encoding='utf-8')


#print("-------------------2---------------------------")
#2 10knn Sam distancia Euclidiana q recomendaria
#recomendador = Recomendador(users, k=7, metric='euclidiana', n=5)
#print(recomendador.recommend("Sam"))


#print("-------------------3---------------------------")
#3 5knn veronica q recomendaria
#recomendador = Recomendador(users, k=5, metric='pearson', numRecomendaciones=5)
#print(recomendador.recommend("Veronica"))

#4 que puntaje bill le dara a The strokes usando (Influencia, puntaje proy)
'''
recomendador = Recomendador(users, k=4, metric='coseno', n=5)
print(recomendador.porcentajeProyectado("Bill", "The Strokes"))

recomendador = Recomendador(users, k=4, metric='pearson', n=5)
print(recomendador.porcentajeProyectado("Bill", "The Strokes"))

#####
recomendador = Recomendador(users, k=3, metric='pearson', n=5)
print(recomendador.porcentajeProyectado("Hailey", "Phoenix"))

recomendador = Recomendador(users, k=2, metric='coseno', n=5)
print(recomendador.porcentajeProyectado("Hailey", "Slightly Stoopid"))
'''

#clear RAM
#su -c "echo 3 >'/proc/sys/vm/drop_caches' && swapoff -a && swapon -a && printf '\n%s\n' 'Ram-cache and Swap Cleared'" root