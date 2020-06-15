import codecs 
import operator
import time
import math
from math import sqrt
import pickle
from collections import defaultdict


def GuardarBinario(obj, name ):
    with open('pkl_files/'+ name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def CargarBinario( name ):
    with open('pkl_files/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)

def arrayToList(s1):
    _list=set()
    for i in range(len(s1)):
        if(s1[i]!=''):
            _list.add(s1[i])
    return _list

def jaccard_distance(ratingsUser, ratingsUser2) : 
    s1 =set()
    s2=set()

    for key,value in ratingsUser.items():
        s1.add(value)
    for key,value in ratingsUser2.items():
        s2.add(value)
        
    size_s1 = len(s1)
    size_s2 = len(s2)
    intersect = s1 & s2
    union = s1.union(s2)
        #print(s1,"-",s2," I=",intersect," U=",union)
    size_in = len(intersect)
    size_un = len(union)
        
    jaccard_in = (size_un-size_in)/size_un
    jaccard_dist = 1 - jaccard_in;
    return jaccard_in,jaccard_dist

def DistancaMinkowski(ratingsUser, ratingsUser2,r):
    distancia=0;
    for key,value in ratingsUser.items():
        if key in ratingsUser2:
            s1=ratingsUser[key]
            s2=ratingsUser2[key]
            s3=s1-s2
            distancia+=pow(abs(s3),int(r))
    #if(distancia==0):
     #   return "ambos usarios no tienen items similares calificados"        
    return pow(distancia,1/int(r))

def DistancaMinkowski_mp(ratingsUser, ratingsUser2):
        r=1
        sumatoria=0;
        for key in ratingsUser[1]:
             if key in ratingsUser2[1]:
                s1=ratingsUser[1][key]
                s2=ratingsUser2[1][key]
                s3=s1-s2
     
                sumatoria+=pow(abs(s3),r)
        return (ratingsUser2[0],pow(sumatoria,1/r))

def similitudCoseno_(ratingsUser, ratingsUser2):
    xoy = 0
    normax = 0
    normay = 0
    for key,value in ratingsUser.items():
        normax+=pow(ratingsUser[key],2)
        if key in ratingsUser2:
            xoy+=ratingsUser[key]*ratingsUser2[key]
            
    if(xoy==0):
        return "ambos usarios no tienen items similares calificados  o son iguales" 
    for key,value in ratingsUser2.items():
        normay+=pow(ratingsUser2[key],2)
    normax = sqrt(normax)
    normay = sqrt(normay)
    
    
    
    if normax*normay==0: 
        return (ratingsUser2,0)
    return (xoy/(normax*normay))

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
    if normax*normay==0: return (ratingsUser2[0],0)
    return (ratingsUser2[0],xoy/(normax*normay))

def pearson(ratingsUser, ratingsUser2):
    print(ratingsUser,"\n",ratingsUser2)
    sum_xy = 0
    sum_x = 0
    sum_y = 0
    sum_x2 = 0
    sum_y2 = 0
    n = 0
    for key,value in ratingsUser.items():
        if key in ratingsUser2:
            n += 1
            x = ratingsUser[key]
            y = ratingsUser2[key]
            sum_xy += x * y
            sum_x += x
            sum_y += y
            sum_x2 += pow(x, 2)
            sum_y2 += pow(y, 2)
    if n == 0:
        return "ambos usarios no tienen items similares calificados  o son iguales"
        
    denominator = (sqrt(sum_x2-pow(sum_x, 2)/n) * sqrt(sum_y2-pow(sum_y, 2)/n))
    if denominator == 0:
        distance = 0
    else:
        distance = (sum_xy - (sum_x * sum_y) / n) / denominator
    print("cerooooo")
    return (distance)

def pearson_mp(ratingsUser, ratingsUser2):
    #print("-------------------------")
    #print(ratingsUser2)
    
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
    #print(ratingsUser2[0], distance,"-------------------------")
    return (ratingsUser2[0], distance)

def distanciaManhattan_mp(ratingsUser, ratingsUser2):
    distancia = 0
    enComun = False
    for key in ratingsUser[1]:
        if key not in ratingsUser[1]: 
            continue
        if key in ratingsUser2[1]:
            distancia += abs(ratingsUser[1][key] - ratingsUser2[1][key])
    
    #print(ratingsUser2[0], distancia,"-------------------------")
    return (ratingsUser2[0], distancia)

def distanciaManhattan(ratingsUser, ratingsUser2):
    distancia = 0
    enComun = False
    for key,value in ratingsUser.items():
        if key in ratingsUser2:
            distancia += abs(float(ratingsUser[key]) - float(ratingsUser2[key]))
    if(distancia==0):
        return "ambos usarios no tienen items similares calificados  o son iguales"
    
    return (distancia)

def distanciaEuclidiana(ratingsUser, ratingsUser2):
    distancia = 0
    #print("distancia ",ratingsUser)
    
    for key,value in ratingsUser.items():
        if key in ratingsUser2:
            distancia += math.pow((float(value) - float(ratingsUser2[key])),2)
            #print(distancia)
    #if(distancia==0):
     #   return "ambos usarios no tienen items similares calificados o son iguales" 
    d=sqrt(distancia)
    return (d)

def distanciaEuclidiana_mp(ratingsUser, ratingsUser2):
    distancia = 0#ratingsUser[0] usuario, ratingsUser[1]=rating

    for key in ratingsUser[1]:
        if key in ratingsUser2[1]:
            distancia += math.pow((ratingsUser[1][key] - ratingsUser2[1][key]),2)
            #print(distancia," = ",ratingsUser[1][key],ratingsUser2[1][key])     
    #print(ratingsUser2[0], math.sqrt(distancia),"Euclidiana----")

    
    return (ratingsUser2[0], math.sqrt(distancia))



def computeSimilarityAllMatriz(listNamesBandas,userRatings):
    
    longitud=len(listNamesBandas)
    matriz={}
    averages = {}
    for (key, ratings) in userRatings.items():
        averages[key] = (float(sum(ratings.values()))
                         /len(ratings.values()))
    print('\n\n',averages,'\n','\n')
    
    for x in range(longitud-1):
        for y in range(x+1,longitud):
            if(x!=y):
                band1=listNamesBandas[x]; band2=listNamesBandas[y]
                num = 0 
                dem1 = 0 
                dem2 = 0
               # print(band1,band2)
                for (user, ratings) in userRatings.items():
                    if band1 in ratings and band2 in ratings:
                        avg = averages[user]
                        num += (ratings[band1] - avg) * (ratings[band2] - avg)
                        #if(x==0):
                         #   print((ratings[band1] ,'-', avg), "*" ,(ratings[band2] ,'-',avg))
                        dem1 += (ratings[band1] - avg)**2
                        dem2 += (ratings[band2] - avg)**2
                d=(sqrt(dem1) * sqrt(dem2))
                #if(x==0):
                 #   print(num)
                if(d==0):
                    result=0
                else:
                    result=num / (sqrt(dem1) * sqrt(dem2))
                #print("->",band1,band2,'\t',result )
                
                if band1 in matriz:
                    currentRatings = matriz[band1]
                else:
                    currentRatings = {}
                currentRatings[band2] = result
                matriz[band1] = currentRatings
    #print(matriz)
    return matriz


def RatingcomputeSimilar(user1,band,listNamesBandas,userRatings):
    #'Patrick C', 'Alien',listaNombresBD, '
    longitud=len(listNamesBandas)
    matriz={}
    averages = {}
    for (key, ratings) in userRatings.items():
        averages[key] = (float(sum(ratings.values()))
                         /len(ratings.values()))
    #print('\n\n',averages,'\n','\n')
    print("total de usuarios",longitud);
    denominador2=0
    iii=0
    for x,y in listNamesBandas.items():
        num = 0 # numerator
        dem1 = 0 # first half of denominator
        dem2 = 0
        band1=listNamesBandas[band]; band2=y
        iii+=1
        #print(iii)
        if(band1!=band2):
            for (user, ratings) in userRatings.items():
                if band1 in ratings and band2 in ratings:
                    avg = averages[user]
                    num += (ratings[band1] - avg) * (ratings[band2] - avg)
                    dem1 += (ratings[band1] - avg)**2
                    dem2 += (ratings[band2] - avg)**2
            cen=(sqrt(dem1) * sqrt(dem2))
            if(cen==0):
                result=0
            else:
                result=( num / cen)
            denominador2+=abs(result)
            matriz[band2] = result
    print("matriz calculada")
    maxNumber=max(userRatings[user1].items(), key=operator.itemgetter(1))[1]
    minNumber=min(userRatings[user1].items(), key=operator.itemgetter(1))[1]
    print("hallando maximos y minimos")
    #print("max number is = ",maxNumber,minNumber);
    normalizeData={}
    for i,x in userRatings[user1].items():
        normalizeData[i]=(2*(x-minNumber)-(maxNumber-minNumber))/(maxNumber-minNumber)
    print("data normalizada")    
    numerador2=0
    #Predecir el puntaje que X dará a Y
    for i,x in normalizeData.items():
        print(i,x)
        if(i==listNamesBandas[band]):
            print("el usuario ya califico a esta banda")
            return "el usuario ya califico a esta banda"
        if(i in matriz):
            #print("-<> ",matriz[i],"|",normalizeData[i])
            numerador2+=matriz[i]*normalizeData[i]
            
    p=numerador2/denominador2
    #print("numerador= ",numerador2,"denominador= ",denominador2);
    print(p)
    #desnormalizar
    resultadorFinal=0.5*((p+1)*(maxNumber-minNumber))+minNumber;
    #print("resultado final = ",resultadorFinal)
    return resultadorFinal

def RatingcomputeSimilarOnlineForMovierating(user1,band,listNamesBandas,userRatings):
    #'Patrick C', 'Alien',listaNombresBD, '
    longitud=len(listNamesBandas)
    matriz={}
    averages = {}
    for (key, ratings) in userRatings.items():
        averages[key] = (float(sum(ratings.values()))
                         /len(ratings.values()))
    #print('\n\n',averages,'\n','\n')
    print("total de usuarios",longitud);
    denominador2=0
    iii=0
    for x in range(len(listNamesBandas)):
        num = 0 # numerator
        dem1 = 0 # first half of denominator
        dem2 = 0
        band1=band; band2=listNamesBandas[x]
        iii+=1
        #print(iii)
        if(band1!=band2):
            for (user, ratings) in userRatings.items():
                if band1 in ratings and band2 in ratings:
                    avg = averages[user]
                    num += (ratings[band1] - avg) * (ratings[band2] - avg)
                    dem1 += (ratings[band1] - avg)**2
                    dem2 += (ratings[band2] - avg)**2
            cen=(sqrt(dem1) * sqrt(dem2))
            if(cen==0):
                result=0
            else:
                result=( num / cen)
            denominador2+=abs(result)
            matriz[band2] = result
    print("matriz calculada")
    maxNumber=max(userRatings[user1].items(), key=operator.itemgetter(1))[1]
    minNumber=min(userRatings[user1].items(), key=operator.itemgetter(1))[1]
    print("hallando maximos y minimos")
    #print("max number is = ",maxNumber,minNumber);
    normalizeData={}
    for i,x in userRatings[user1].items():
        normalizeData[i]=(2*(x-minNumber)-(maxNumber-minNumber))/(maxNumber-minNumber)
    print("data normalizada")    
    numerador2=0
    #Predecir el puntaje que X dará a Y
    for i,x in normalizeData.items():
        print(i,x)
        if(i==band):
            print("el usuario ya califico a esta banda")
            return "el usuario ya califico a esta banda"
        if(i in matriz):
            #print("-<> ",matriz[i],"|",normalizeData[i])
            numerador2+=matriz[i]*normalizeData[i]
            
    p=numerador2/denominador2
    #print("numerador= ",numerador2,"denominador= ",denominador2);
    print(p)
    #desnormalizar
    resultadorFinal=0.5*((p+1)*(maxNumber-minNumber))+minNumber;
    #print("resultado final = ",resultadorFinal)
    return resultadorFinal

def loadBookDB(path=''):
        """loads the BX book dataset. Path is where the BX files are
        located"""
        data = {}
        username2id = {}
        userid2name = {}
        productid2name = {}
        
        i = 0
        f = codecs.open(path + "BX-Book-Ratings.csv", 'r', 'utf8')
        for line in f:
            i += 1
            #separate line into fields
            fields = line.split(';')
            user = fields[0].strip('"')
            book = fields[1].strip('"')
            rating = float(fields[2].strip().strip('"'))
            if(rating!=0):
                if user in data:
                    currentRatings = data[user]
                else:
                    currentRatings = {}
                currentRatings[book] = rating
                data[user] = currentRatings
                
        f.close()
        GuardarBinario(data, "Book_data")
        #
        # Now load books into self.productid2name
        # Books contains isbn, title, and author among other fields
        #
        f = codecs.open(path + "BX-Books.csv", 'r', 'utf8')
        nombres={}
        for line in f:
            i += 1
            #separate line into fields
            fields = line.split(';')
            isbn = fields[0].strip('"')
            title = fields[1].strip('"')
            nombres[title]=isbn
            author = fields[2].strip().strip('"')
            title = title + ' by ' + author
            productid2name[isbn] = title
        f.close()
        GuardarBinario(nombres,"Book_names")
        GuardarBinario(productid2name, "Book_productid2name")
        #
        #  Now load user info into both self.userid2name and
        #  self.username2id
        #
        f = codecs.open(path + "BX-Users.csv", 'r', 'utf8')
        for line in f:
            i += 1
            #print(line)
            #separate line into fields
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
            userid2name[userid] = value
            username2id[location] = userid
        f.close()
        GuardarBinario(userid2name, "Book_userid2name")
        GuardarBinario(username2id, "Book_username2id")
        print(i)

def loadMoviLens(path=''):
        username2id = {}
        userid2name = {}
        productid2name = {}
        data={}
        data2 = {}
        #data2= CargarBinario("movilens1k_data")
        print(data2)
        i = 0
        print("----------")
        f = codecs.open(path + "ml-latest-small/ratings.csv", 'r', 'utf8')
        for line in f:
            i += 1
            fields = line.split(',')
            user = fields[0].strip('"')
            movie = fields[1].strip('"')
            rating = float(fields[2].strip().strip('"'))
            if user in data:
                currentRatings = data[user]
            else:
                currentRatings = {}
            currentRatings[movie] = rating
            data[user] = currentRatings
        GuardarBinario(data, "movilens1k_data")
        f.close()

        
        ff = codecs.open(path + "ml-latest-small/movies.csv", 'r', 'utf8')
        nombres={}
        for line in ff:
            i += 1
            fields = line.split(',')
            
            movieId = fields[0].strip('"')
            title = fields[1].strip('"')
            
            nombres[title]=movieId
            genre = fields[2].strip().strip('"')
            title = title + ', ' + genre
            productid2name[movieId] = title
        GuardarBinario(productid2name,"movilens1k_productid2name")
        GuardarBinario(nombres,"movielens_namePeliculas")
        
    
        ff.close()
        print("----------")

def loadMoviLens27():
        data = {}
        productid2name = {}
        #self.data = self.load_obj("ratings27m")
        nombres={}
        print("----------")
        f = codecs.open("ml-latest/movies.csv", 'r', 'utf8')
        for line in f:
            fields = line.split(',')
            movieId = fields[0].strip('"')            
            title = fields[1].strip('"')
            nombres[title]=movieId
            genre = fields[2].strip().strip('"')
            title = title + ', ' + genre
            productid2name[movieId] = title
        f.close()
        GuardarBinario(productid2name, "movilens27k_productid2name")
        GuardarBinario(nombres,"movielens27k_namePeliculas")
        #self.productid2name = self.load_obj("product_movies27m")

        print("----------")
        #self.data = self.load_obj("ratings27m")
        f = codecs.open("ml-latest/ratings.csv", 'r', 'utf8')
        print("archivo abierto")
        for line in f:
            try:
                fields = line.split(',')
                user = fields[0].strip('"')
                movie = fields[1].strip('"')
                rating = float(fields[2].strip().strip('"'))
                if(rating!=0):
                    print(rating)
                    if user in data:
                        currentRatings = data[user]
                    else:
                        currentRatings = {}
                    currentRatings[movie] = rating
                    data[user] = currentRatings
            except Exception:
                continue
        print("archivo cerradp")
        GuardarBinario(data, "movilens27k_data")
        f.close()


def loadMovieRating(path=''):
        data = {}
        user=[]
        x=0
        f = codecs.open(path + "Data/Movie_Ratings.csv", 'r', 'utf8')
        
        namePeliculas=[]
        for line in f:
            #print(line)
            fields = line.split(',')
            if(fields[0]!=''):
                namePeliculas.append(fields[0]);
            for i in range(len(fields)):
                if(x==0):
                    if(i==len(fields)-1):                
                        nombre=fields[i].strip()
                        #print(nombre)
                        user.append(nombre)
                        currentRatings={}
                        data[nombre] = currentRatings
                    else:
                        if(i!=0):   
                            nombre=fields[i]
                            
                            user.append(nombre)
                            currentRatings={}
                            data[nombre] = currentRatings
                else:
                    if(i==len(fields)-1):     
                        #print(i,len(user)-1)
                        nombre=fields[i].strip()
                        currentRatings = data[user[i-1]]
                        if(nombre==''):
                            nombre=0
                        else:
                            currentRatings[fields[0]]=float(nombre)
                        data[user[i-1]] = currentRatings
                    else: 
                        if(i!=0):
                            nombre=fields[i]
                            #print(nombre)
                            currentRatings = data[user[i-1]]
                            if(nombre==''):
                                nombre=0
                            else:
                                currentRatings[fields[0]]=float(nombre)
                            data[user[i-1]] = currentRatings
            x+=1
           
        f.close()
        GuardarBinario(data, "Movie_Ratings")
        GuardarBinario(namePeliculas, "Movie_Ratings_namePeliculas")
        
        '''for i,k in data.items():    
            print(i,k)'''
        
        
if __name__=="__main__":
    users3 = {"David": {"2": 3, "3": 5,
             "4": 4, "5": 1},
             "Matt": {"2": 3, "3": 4,
             "4": 4, "5": 1},
             "Ben": {"1": 4, "2": 3,
             "4": 3, "5": 1},
             "Chris": {"1": 4, "2": 4,
             "3": 4, "4": 3, "5": 1},
             "Tori": {"1": 5, "2": 4,
             "3": 5, "5": 3}}
                
    userRatings = CargarBinario("Movie_Ratings")
    listaNombresBD =CargarBinario("Movie_Ratings_namePeliculas")
    
    RatingcomputeSimilarOnlineForMovierating('Patrick C', 'Alien',listaNombresBD, userRatings)
    
    listaNombresBD = {'Kacey Musgraves':'1','Imagine Dragons':'2','Daft Punk':'3', '4':'4','Fall Out Boy':'5'}
    RatingcomputeSimilar('David', 'Kacey Musgraves',listaNombresBD, users3)
    
<<<<<<< HEAD:testing/Distancias.py
    
=======
>>>>>>> a0a939f541e2e2dfa39a5995037d4f388474f541:Distancias.py
    #loadMoviLens27()
    #loadMovieRating()
    #loadBookDB("dataBook/")
    #loadMoviLens()
    
    
    
    
    
    
