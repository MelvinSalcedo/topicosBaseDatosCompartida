import codecs 
from math import sqrt
from time import perf_counter 
import pickle
import sys
from PyQt5 import uic
from PyQt5.QtWidgets import QApplication, QMainWindow
import time
import multiprocessing
import multiprocessing
import os
from multiprocessing import Queue
from multiprocessing import Pool
from multiprocessing import Process, Manager
import itertools
import Distancias as fl


class recommender:

    def __init__(self,k=1,metric='Pearson'):
        self.listaNombresBD=[]
        self.usuario_existe=1
        self.k = k
        self.nunmeroRecomendaciones=20
        self.totalUsuarios=0
        self.username2id = {}
        self.userid2name = {}
        self.productid2name = {}
        self.metric = metric
        self.VecinosCercanos=[]
        self.data = {}
        self.rr=1
            
    def GuardarBinario(self,obj, name ):
        with open('pkl_files/'+ name + '.pkl', 'wb') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

    def CargarBinario(self, name ):
        with open('pkl_files/' + name + '.pkl', 'rb') as f:
            return pickle.load(f)

    def convertProductID2name(self, id):
        """Given product id number return product name"""
        if id in self.productid2name:
            return self.productid2name[id]
        else:
            return id

    def userRatings(self, id, n):
        """Return n top ratings for user with id"""
        print ("Ratings for " + self.userid2name[id])
        ratings = self.data[id]
        print(len(ratings))
        ratings = list(ratings.items())
        ratings = [(self.convertProductID2name(k), v)
                   for (k, v) in ratings]
        # finally sort and return
        ratings.sort(key=lambda artistTuple: artistTuple[1],
                     reverse = True)
        ratings = ratings[:n]
        for rating in ratings:
            print("%s\t%i" % (rating[0], rating[1]))


    def loadBookDB(self, path=''):
        print("________________")
        print("begin load data")
        self.data={}
        self.productid2name={}
        self.data = self.CargarBinario("Book_data")
        self.username2id = self.CargarBinario("Book_userid2name")
        self.userid2name = self.CargarBinario("Book_username2id")
        self.productid2name = self.CargarBinario("Book_productid2name")  
        self.listaNombresBD = self.CargarBinario("Book_names")  
        print("end load  data")
        
        
    def loadMovieLensN(self):
        self.data={}
        self.productid2name={}
        print("________________")
        self.data = fl.CargarBinario("movilens1k_data")
        self.productid2name = fl.CargarBinario("movilens1k_productid2name")
        self.listaNombresBD =fl.CargarBinario("movielens_namePeliculas")
        print(self.listaNombresBD)
        
        print("________________")
    def loadMovieLens27m(self):
        self.data={}
        self.productid2name={}
        print("________________")
        self.data = fl.CargarBinario("movilens27k_data")
        print("________________")
        self.productid2name = fl.CargarBinario("movilens27k_productid2name") 
        self.listaNombresBD =fl.CargarBinario("movielens27k_namePeliculas")
        print("fin de carga de la data")
        
    def loadMovieRating(self):
        self.data={}
        self.productid2name={}
        self.listaNombresBD=[]
        
        print("________________")
        self.data = fl.CargarBinario("Movie_Ratings")  
        self.listaNombresBD = fl.CargarBinario("Movie_Ratings_namePeliculas")  
        #print(self.listaNombresBD)
        print("________________")

    def ususarioExiste(self, username):
        try:
            self.data[username]
        except:
            print("USUARIO NO EXISTE")
            self.usuario_existe=-1
            return -1
            
    def cacularDistanciasKnn(self, username):
        if(self.ususarioExiste(username)==-1):
            return
        
        usuarios = list(self.data.keys())
        tamTotal = len(usuarios)

        print("Total de usuarios:", tamTotal)
        self.username = username   
        ratingsUser = [username,self.data[username]]
        
        dictlist = []
        dictlist = [ (ratingsUser, [k,v]) for k, v in self.data.items() if k != username]
        #print(dictlist)
        number_of_workers = 8
        
        with Pool(number_of_workers) as p:
            if (self.metric == "Minkowski"):
                distances = p.starmap(fl.DistancaMinkowski_mp,dictlist)
            elif (self.metric == "Pearson"):
                distances = p.starmap(fl.pearson_mp, dictlist)
            elif (self.metric == "Manhatan"):
                distances = p.starmap(fl.distanciaManhattan_mp, dictlist)
            elif (self.metric == "Euclidiana"):
                distances = p.starmap(fl.distanciaEuclidiana_mp, dictlist)
            elif (self.metric == "Coseno"):
                distances = p.starmap(fl.similitudCoseno_mp, dictlist)
                
            
        #print("D= ",distances)
        if self.metric == "Pearson":
            distances.sort(key=lambda artistTuple: artistTuple[1], reverse=True) # ordenar basado en la distancia, de menor a mayor
        elif self.metric == "Manhatan":
            distances.sort(key=lambda artistTuple: artistTuple[1]) # ordenar basado en la distancia, de menor a mayor
            dis=self.getNearestDiferentCero(distances)
            return dis
        elif self.metric == "Euclidiana":
            distances.sort(key=lambda artistTuple: artistTuple[1]) # ordenar basado en la distancia, de menor a mayor
            dis=self.getNearestDiferentCero(distances)
            return dis
        elif self.metric == "Minkowski":
            distances.sort(key=lambda artistTuple: artistTuple[1]) # ordenar basado en la distancia, de menor a mayor
            dis=self.getNearestDiferentCero(distances)
            return dis
        elif self.metric == "Coseno":
            distances.sort(key=lambda artistTuple: artistTuple[1], reverse=True) # ordenar basado en la distancia, de menor a mayor # ordenar basado en la distancia, de menor a mayor

        return distances

    def vecinoscercarnosMostrar(self,user,kk):
        nearest = self.cacularDistanciasKnn(user)
        #print("nearest",nearest)
        strs=''
        i=0
        for key,value in nearest:
            if(i<=kk):
                #print(value)
                strs+="["+key+','+str(value)+"] "
            i+=1
        return strs

    def getNearestDiferentCero(self,nearest):
        vecino=[]
        x=0

        for i in range(len(nearest)):
            #print(nearest[i][0],nearest[i][1])
            if(nearest[i][1]!=0):
                b = (nearest[i][0],nearest[i][1])
                vecino.append(b)
                x=x+1
                if(x==self.k):
                    return vecino
        return vecino
            
    
    def recommend(self, user):
        if(self.ususarioExiste(user)==-1):
            return

        recommendations = {}
        nearest = self.cacularDistanciasKnn(user)
        userRatings = self.data[user]
        print("nearest\n",nearest)
        totalDistance = 0.0
        
        for i in range(self.k):
            totalDistance += nearest[i][1]
            print(nearest[i][1])
        print("distancai total de ususarios k",totalDistance,self.k)
       # now iterate through the k nearest neighbors
       # accumulating their ratings
        if(totalDistance==0):
            return ("no puede ser divido por cero")
        
        print("totalDistance = ",totalDistance,'\n')
        for i in range(self.k):
            # compute slice of pie 
            weight = nearest[i][1] / totalDistance
            
            # get the name of the person
            name = nearest[i][0]
            # get the ratings for this person
            neighborRatings = self.data[name]
          # get the name of the person
          # now find bands neighbor rated that user didn'
            for artist in neighborRatings:
                if not artist in userRatings:
                    if artist not in recommendations:
                        recommendations[artist] = (neighborRatings[artist]
                                              * weight)
                        print("WEIGHT=",neighborRatings[artist], weight,"=",(neighborRatings[artist]
                                              * weight))
                    else:
                        recommendations[artist] = (recommendations[artist]
                                              + neighborRatings[artist]
                                              * weight)
        # now make list from dictionary
        print("\nRecomendatios = ",recommendations)
                        
        recommendations = list(recommendations.items())
        recommendations = [(self.convertProductID2name(k), v)
                          for (k, v) in recommendations]
       # finally sort and return
        recommendations.sort(key=lambda artistTuple: artistTuple[1],
                            reverse = True)
       # Return the first n items
        return recommendations[:len(recommendations)]


class ejemplo_GUI(QMainWindow):
    """docstring for ejemplo_GUI"""
    def __init__(self):
        super().__init__()
        uic.loadUi("GUI2.ui",self)
        self.baseDatos=None
        
        self.boton_distancia.clicked.connect(self.IniciarConsultaDistancia)
        self.boton_recomendacion.clicked.connect(self.IniciarConsultaRecomendacion)
        self.boton_knn.clicked.connect(self.IniciarConsultaKnn)
        self.Updatebd.clicked.connect(self.ChangeBaseDatos)
        self.boton_CA.clicked.connect(self.CalcularCA)
        
        self.tipoBaseDatos=0
        self.k=self.knn.text()
        self.objetoClaseRecomender = recommender(int(self.k))
        self.ChangeBaseDatos() 
        self.tipeBase=0
        
    def ChangeBaseDatos(self):
        if(self.MovieLens.isChecked()):
            self.tipeBase=0
            self.objetoClaseRecomender.loadMovieLensN()
            
        elif(self.book.isChecked()):
            self.tipeBase=4
            self.objetoClaseRecomender.loadBookDB()
            
        elif(self.MovieRating.isChecked()):
            self.tipeBase=1
            self.objetoClaseRecomender.loadMovieRating()
            
        elif(self.MovieLens27.isChecked()):
            self.tipeBase=3
            self.objetoClaseRecomender.loadMovieLens27m()
        print("data cambiada",self.tipeBase)
            
        
            

    def IniciarConsultaDistancia(self):
        texto=0
        distancia=None
        p11=self.Usuario1.text()
        p22=self.Usuario2.text()
        rr1=self.r_minkowski.text();
        
        if(self.objetoClaseRecomender.ususarioExiste(p11)==-1):
            self.textEdit.setText("El usuario "+p11+ " no existe")
            return
        
        if(self.objetoClaseRecomender.ususarioExiste(p22)==-1):
            self.textEdit.setText("El usuario "+p22+ " no existe")
            return
        
        print("p22 = ",self.objetoClaseRecomender.data[p22])
        p1 = self.objetoClaseRecomender.data[p11]
        p2 = self.objetoClaseRecomender.data[p22]


        if(self.DistanciaE.isChecked()):
            distancia = fl.distanciaEuclidiana(p1,p2)
        if(self.DistanciaM.isChecked()):
            distancia = fl.distanciaManhattan(p1,p2)
        if(self.Minkowski.isChecked()):
            distancia = fl.DistancaMinkowski(p1,p2,rr1)
        if(self.DistanciaC.isChecked()):
            distancia = fl.similitudCoseno_(p1,p2)
        if(self.Pearson.isChecked()):
            distancia = fl.pearson(p1,p2)
        if(self.Jaccard.isChecked()):
            distancia = fl.jaccard_distance(p1,p2)
        
        if(self.CosenoAjustado.isChecked()):
            texto=1
            distancia = fl.computeSimilarity(self.objetoClaseRecomender.listaNombresBD,
                                             self.objetoClaseRecomender.data)
        if(texto==1):
            self.textEdit.clear()
            for i,x in distancia.items():
                pair=None
                self.textEdit.append(i)
                for j,l in x.items():
                    pair="\t"+str(j)+" "+str(l)
                    self.textEdit.append(pair)
        else:
            self.textEdit.setText(str(distancia))
        

    def IniciarConsultaRecomendacion(self):
        if(self.DistanciaE.isChecked()):
            self.objetoClaseRecomender.metric="Euclidiana"
        if(self.DistanciaM.isChecked()):
            self.objetoClaseRecomender.metric="Manhatan"
        if(self.Minkowski.isChecked()):
            self.objetoClaseRecomender.metric="Minkowski"
        if(self.DistanciaC.isChecked()):
            self.objetoClaseRecomender.metric="Coseno"
        if(self.Pearson.isChecked()):
            self.objetoClaseRecomender.metric="Pearson"

        k=self.knn.text()
        p1=self.Usuario1.text()
        
        if(self.objetoClaseRecomender.ususarioExiste(p1)==-1):
            self.textEdit.setText("El usuario "+p1+ " no existe")
            return
        self.objetoClaseRecomender.k=int(k)
        KnnName = self.objetoClaseRecomender.recommend(str(p1))  
        
        listaR=[]
        self.textEdit.clear()
        for i in range(len(KnnName)):
            pair=str(KnnName[i][0])+"\t\t"+str(KnnName[i][1])
            self.textEdit.append(pair)
        #self.textEdit.setText(listaR)

        
    def IniciarConsultaKnn(self):
        if(self.DistanciaE.isChecked()):
            self.objetoClaseRecomender.metric="Euclidiana"
            
        if(self.DistanciaM.isChecked()):
            self.objetoClaseRecomender.metric="Manhatan"
            
        if(self.Minkowski.isChecked()):
            self.objetoClaseRecomender.metric="Minkowski"
            
        if(self.DistanciaC.isChecked()):
            self.objetoClaseRecomender.metric="Coseno"
            
        if(self.Pearson.isChecked()):
            self.objetoClaseRecomender.metric="Pearson"
        
        k=self.knn.text()
        p1=self.Usuario1.text()
        
        if(self.objetoClaseRecomender.ususarioExiste(p1)==-1):
            self.textEdit.setText("El usuario "+p1+ " no existe")
            return
        p=self.objetoClaseRecomender.vecinoscercarnosMostrar(p1,int(k)-1)

        self.textEdit.setText(p)


    def CalcularCA(self):
        distancia=None
        p11=self.Usuario1.text()
        p22=self.Usuario2.text()
        
        if(self.objetoClaseRecomender.ususarioExiste(p11)==-1):
            self.textEdit.setText("El usuario "+p11+ " no existe")
            return
        #RatingcomputeSimilar('1', 'Toy Story (1995)',listaNombresBD, userRatings)
        if(self.CosenoAjustado.isChecked() and self.tipeBase==0):
            distancia = fl.RatingSimilitudCoseno(p11,p22,self.objetoClaseRecomender.data)
            self.textEdit.setText(str(distancia))
        
        if(self.CosenoAjustado.isChecked() and self.tipeBase==1):
            print("Patrick C")
            distancia = fl.RatingSimilitudCoseno(p11,p22,self.objetoClaseRecomender.self.objetoClaseRecomender.data)
            self.textEdit.setText(str(distancia))
            
        if(self.CosenoAjustado.isChecked() and self.tipeBase==4 or self.tipeBase==3):
            distancia = fl.RatingSimilitudCoseno(p11,p22,self.objetoClaseRecomender.data)
            self.textEdit.setText(str(distancia))
            
if __name__ == "__main__":
    
    app = QApplication(sys.argv)
    GUI = ejemplo_GUI()
    GUI.show()
    sys.exit(app.exec_())
    #r = recommender()
    #r.loadMovieLens27m()
    '''a = perf_counter()  
    r.loadBookDB()
    #r.loadMovieLensN()
    #r.loadMovieLens27m()
    print(r.recommend('276704'))
    b = perf_counter()  
    print(r.totalUsuarios)
    print ("Tiempo de ejecucion = ",b-a,)'''