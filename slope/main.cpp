#include <iostream>
#include <fstream>
#include <cmath>
#include <map>
#include <thread>
#include <fstream>
#include <string>
#include <iostream>
#include <ctime>
#include <algorithm>
#include <vector>

using namespace std;

#define NUM_THREADS 8
#define MAX_RATING 5
#define NULL_VAL -1

typedef double ValType;
typedef int IndexType;
typedef ValType Valoration;
typedef vector<ValType> ValVec;
typedef int MovieId;
typedef string MovieName;
typedef string MovieCategories;
typedef tuple<MovieName, MovieCategories> MovieRegister;

typedef int UserId;
typedef tuple<MovieId, Valoration> InterRegister;

typedef map<MovieId, Valoration> InterRegisterMap;
typedef map<int, tuple<ValType, float>>  MovieLensVectorDesviacion;

typedef vector<string> Registro;

ValType stoVT(string number){
	return stof(number);
}

vector<string> splitString(string str, char delimit){
	vector<string> res;
	string word = "";
	for(char c : str){
		if(c == delimit){
			res.push_back(word);
			word.clear();
		}
		else word.push_back(c);
	}	
	res.push_back(word);
	return res;
}

void printRegistro(Registro reg){
	for(string s : reg){
		cout<<s<<" ";
	}
	cout<<endl;
}
class MyTime{
	public:
		MyTime(){}
		void init(){begin_t = clock();}	
		void end(){end_t = clock();}
		double getTime(){return double(end_t - begin_t) / CLOCKS_PER_SEC;}
		void print(){
			double res = double(end_t - begin_t) / CLOCKS_PER_SEC;
			cout<<res<<"s"<<endl;
		}
	private:
		clock_t begin_t;
		clock_t end_t;
};

ValType desviacionEstandar(ValVec & x, ValVec & y){ //tienes que pasar los vectores con los valores que hacen match
	float cardinalidad = x.size();
	ValType res = 0;
	for(int i = 0; i < x.size(); i++){
		res += (x[i] - y[i]) / cardinalidad;
	}
	//cout<<"R->"<<res<<endl;
	//cout<<cardinalidad<<endl;
	return res;
}

MovieLensVectorDesviacion getvectorDesviacion(map<int, map<int,ValType>> & vals, vector<map<int, ValType>> & valsUser, IndexType userId, IndexType movieId){
	MovieLensVectorDesviacion res;
	ValVec a;
	ValVec b;
	auto actualMovie = vals.find(movieId);
	map<int,ValType> * actualUser = &valsUser[userId];
	if(actualMovie == vals.end()){
		cout<<"El libro no tiene ranqkings"<<endl;
		return res;
	}
	map<int,ValType>::iterator findMovie;
	map<int,ValType>::iterator findUser;
	for(auto iter = vals.begin(); iter != vals.end(); ++iter){
		if(actualMovie == iter) continue;
		findMovie = actualUser->find(iter->first);
		if(findMovie == actualUser->end()) continue;
		a.clear();
		b.clear();
		for(auto iter2 = iter->second.begin(); iter2 != iter->second.end(); ++iter2){
			findUser = actualMovie->second.find(iter2->first);
			if(findUser == actualMovie->second.end()) continue;
			a.push_back(findUser->second);
			b.push_back(iter2->second);
		}
		//if(a.size() == 0 or a.size() == 1) continue;
		if(a.size() == 0) continue;
		//cout<<iter->first<<endl;
		res[iter->first] = make_tuple(desviacionEstandar(a,b), a.size());
	}
	return res;
}

vector<ValVec> getBdVals(string file, int n){
	ifstream valsFile(file.c_str());
	vector<ValVec> vals(n);
	string word = "";
	int count = 0;
	while(valsFile>>word){	
		if(word == "-") vals[count].push_back(NULL_VAL);
		else vals[count].push_back(stoVT(word));
		count++;
		if(count == n) count = 0;
	}
	valsFile.close();
	return vals;
}

vector<string> getBdNames(string file){
	vector<string> names;
	ifstream namesFile(file.c_str());
	string word = "";
	while(namesFile>>word){
		names.push_back(word);
	}
	namesFile.close();
	return names;
}

vector<Registro> getBd(string fileName, char delimit){
	vector<Registro> res;
	ifstream bdFile(fileName.c_str());
	string word = "";
	char line[512];
	while(bdFile.getline(line,512)){
		word = string(line);
		res.push_back(splitString(word, delimit));
	}
	bdFile.close();

	return res;
}

ValType slopeOne(MovieLensVectorDesviacion & vec, map<int, map<int,ValType>> & vals, vector<map<int, ValType>> & valsUser, IndexType userId, IndexType movieId){
	ValType n = 0;
	ValType d = 0;
	map<int,ValType> * actualUser = &valsUser[userId];
	MovieLensVectorDesviacion::iterator findRes;
	for(auto iter = actualUser->begin(); iter != actualUser->end(); ++iter){
		findRes = vec.find(iter->first);
		n += (get<0>(findRes->second) + iter->second) * get<1>(findRes->second);
		d += get<1>(findRes->second);
	}
	if(d == 0) return 0;
	return (ValType) n / d;
}

void getSlope(vector<InterRegisterMap> * valsByUser, map<MovieId,map<UserId,ValType>> * valsByProduct, 
				map<MovieId, MovieRegister>::iterator ini, map<MovieId, MovieRegister>::iterator end,
				UserId userId, vector<tuple<MovieId,Valoration>> * valoraciones){
	InterRegisterMap::iterator findRes;
	map<MovieId,map<UserId,ValType>>::iterator findMovieRes;
	MovieLensVectorDesviacion desVec;
	ValType res = 0;
	
	for(auto iter = ini; iter != end; ++iter){	
		findMovieRes = valsByProduct->find(iter->first);
		if(findMovieRes == valsByProduct->end()) continue;
		findRes = (*valsByUser)[userId].find(iter->first);
		if(findRes != (*valsByUser)[userId].end()) continue;

		desVec = getvectorDesviacion(*valsByProduct, *valsByUser, userId, iter->first);
		res = slopeOne(desVec, *valsByProduct, *valsByUser, userId, iter->first);
		//if(res > 5) continue;
		valoraciones->push_back(make_tuple(iter->first,res));
	}
}


void printInters(InterRegisterMap interMap){
	for(auto iter = interMap.begin(); iter != interMap.end(); ++iter){
		cout<<iter->first<<" "<<iter->second<<endl;
	}
}

void printMovie(MovieRegister movie){
	cout<<get<0>(movie)<<" "<<get<1>(movie)<<endl;
}

int main(){
	MyTime mytime;
	cout<<"Cargando Base de Datos..."<<endl;
	mytime.init();
	
	//auto bdMovies = getBd("millones/prebokii.csv", ';');
	//auto bdInter = getBd("millones/BookRating.csv", ';');
	auto bdMovies = getBd("millones/movies.csv", ',');
	auto bdInter = getBd("millones/ratings20.csv", ',');
	cout<<"cargue bien ambos"<<endl;
	
	mytime.end();
	cout<<"Done->";
	mytime.print();

	UserId id = 0;

	cout<<"Generando estructura Movies..."<<endl;
	mytime.init();
	map<MovieId, MovieRegister> movieVec;
	for(auto iter = bdMovies.begin(); iter != bdMovies.end(); ++iter){
		//cout<<"analizando: "<<(*iter)[0]<<endl;
		id = stoi((*iter)[0]);
		//cout<<"se analizooo"<<endl;
		movieVec[id] = make_tuple((*iter)[1], (*iter)[2]);
		//cout<<"lolo"<<endl;
	}
	mytime.end();
	cout<<"Done->";
	mytime.print();


	cout<<"Generando estructura InterbyUser..."<<endl;
	mytime.init();

	int numUsers = 138493;

	vector<InterRegisterMap> valsByUser(numUsers);
	Valoration val = 0;	

	MovieId id2 = 0;

	for(auto iter = bdInter.begin(); iter != bdInter.end(); ++iter){
		//cout<<"eee : "<<(*iter)[0]<<endl;
		//cout<<"aaa:  "<<(*iter)[1]<<endl;
		//cout<<"oo: "<<(*iter)[2]<<endl;
		id = stoi((*iter)[0]) - 1;
		id2 = stoi((*iter)[1]);
		val = stof((*iter)[2]);
		valsByUser[id][id2] = val;
		//cout<<"que fue"<<endl;
	}
	mytime.end();
	cout<<"Done->";
	mytime.print();
	
	cout<<"Generando estructura InterbyProduct..."<<endl;
	mytime.init();

	map<MovieId,map<UserId,ValType>> valsByProduct;
	for(auto iter = bdInter.begin(); iter != bdInter.end(); ++iter){
		id = stoi((*iter)[0]) - 1;
		id2 = stoi((*iter)[1]);
		val = stof((*iter)[2]);
		valsByProduct[id2][id] = val;
	}
	mytime.end();
	cout<<"Done->";
	mytime.print();

	bdInter.clear();
	bdMovies.clear();

	UserId userId = 0;
	MovieId movieId = 0;
	int option = 0;
	int bd = 0;

	while(true){
		cout<<endl<<"1) Busqueda"<<endl;
		cout<<"2) Prediccion"<<endl;
		//cout<<"3) Nuevo Usuario"<<endl;
		//cout<<"4) Iniciar Sesion"<<endl;
		//cout<<"5) Recomendacion1"<<endl;
		//cout<<"6) Recomendacion2"<<endl;
		//cout<<"7) Recomendacion3"<<endl;

		cout<<"Opcion->";
		cin>>option;
		cout<<endl;
		switch(option){
			case 1:{
				cout<<"1) Movie"<<endl;
				cout<<"2) Inter"<<endl;
				cout<<"Opcion->";
				cin>>bd;
				cout<<endl;
				switch(bd){
					case 1:{
						cout<<"Id->";
						cin>>movieId;
						printMovie(movieVec[movieId]);
						break;
					}
					case 2:{
						cout<<"Id->";
						cin>>userId;
						userId--;
						printInters(valsByUser[userId]);
						break;
					}
				}
				break;
			}
			case 2:{
				//ATENCIOOOOONNNN!!!!! el ide del usuario tiene que resarce uno.
				cout<<"UserId->";
				cin>>userId;
				userId--;
				cout<<"MovieId->";
				cin>>movieId;
				auto findRes = valsByUser[userId].find(movieId);
				if(findRes != valsByUser[userId].end()){
					cout<<"El libro ya ha sido ranqueado por este usuario"<<endl;
					break;
				}
				auto desVec = getvectorDesviacion(valsByProduct, valsByUser, userId, movieId);
				ValType res = slopeOne(desVec, valsByProduct, valsByUser, userId, movieId);
				cout<<"El usuario "<<userId<<" pondría el puntaje "<<res<<" al libro "<<get<0>(movieVec[movieId])<<endl;
				break;
			}
			case 3:{
				numUsers++;
				valsByUser.push_back(InterRegisterMap());
				cout<<"Nuevo usuario creado"<<endl;
				cout<<"Id del nuevo usuario->"<<numUsers<<endl;
				break;
			}
			case 4:{
				cout<<"UserId->";
				cin>>userId;
				userId--;
				cout<<"Bienvenido usuario "<<userId + 1<<endl;
				bool flag = true;
				Valoration val = 0;
				while(flag){
					cout<<"1) Mis ranqueados"<<endl;
					cout<<"2) Ranquear pelicula"<<endl;
					cout<<"3) Cerrar Sesion"<<endl;	
					cout<<"Opcion->";
					cin>>option;
					cout<<endl;
					switch(option){
						case 1:{
							printInters(valsByUser[userId]);
							break;
						}
						case 2:{
							cout<<"MovieId->";
							cin>>movieId;
							auto findRes = valsByUser[userId].find(movieId);
							if(findRes != valsByUser[userId].end()){
								cout<<"La pelicula ya ha sido ranqueada por este usuario"<<endl;
								cout<<"Quiere cambiar el puntaje?? 1) Si 2) No"<<endl;
								cout<<"Opcion->";
								cin>>option;
								if(option == 2) break;
							}
							cout<<"Puntaje->";
							cin>>val;
							valsByUser[userId][movieId] = val;
							valsByProduct[movieId][userId] = val;
							cout<<"Pelicula ranqueada correctamente"<<endl<<endl;
							break;
						}
						case 3:{
							flag = false;
							break;
						}
					}
				}
				break;
			}
			case 5:{
				int k;
				cout<<"UserId->";
				cin>>userId;
				userId--;
				cout<<"K->";
				cin>>k;
				vector<tuple<MovieId,Valoration>> valoraciones;
				InterRegisterMap::iterator findRes;
				map<MovieId,map<UserId,ValType>>::iterator findMovieRes;
				MovieLensVectorDesviacion desVec;
				ValType res = 0;
				int count = 0;
				cout<<"Generando recomendaciones...";
				mytime.init();
				for(auto iter = movieVec.begin(); iter != movieVec.end(); ++iter){
					cout<<count++<<"/"<<movieVec.size()<<endl;
					findMovieRes = valsByProduct.find(iter->first);
					if(findMovieRes == valsByProduct.end()) continue;
					findRes = valsByUser[userId].find(iter->first);
					if(findRes != valsByUser[userId].end()) continue;

					desVec = getvectorDesviacion(valsByProduct, valsByUser, userId, iter->first);
					res = slopeOne(desVec, valsByProduct, valsByUser, userId, iter->first);
					if(res > 5) continue;
					valoraciones.push_back(make_tuple(iter->first,res));
				}
				sort(valoraciones.begin(), valoraciones.end(), [](tuple<MovieId,Valoration> a, tuple<MovieId, Valoration> b){
					return get<1>(a) > get<1>(b);
				});
				if(valoraciones.size() > k) valoraciones.erase(valoraciones.begin() + k, valoraciones.end());
				for(auto resTuple : valoraciones){
					//cout<<get<1>(resTuple)<<" -> "<<get<0>(movieVec[get<0>(resTuple)])<<endl;
				}
				mytime.end();
				cout<<"Done->";
				mytime.print();
				break;
			}
			case 6:{
				int k;
				cout<<"UserId->";
				cin>>userId;
				userId--;
				cout<<"K->";
				cin>>k;
				vector<tuple<MovieId,Valoration>> valoraciones;
				vector<tuple<MovieId,Valoration>> valoracionesRes;
				InterRegisterMap::iterator findRes;
				map<MovieId,map<UserId,ValType>>::iterator findMovieRes;
				MovieLensVectorDesviacion desVec;
				ValType res = 0;
				int count = 0;
				cout<<"Generando recomendaciones..."<<endl;
				mytime.init();
				for(auto iter = movieVec.begin(); iter != movieVec.end(); ++iter){
					if(valoracionesRes.size() == k) break;
					//cout<<count++<<"/"<<movieVec.size()<<endl;
					findMovieRes = valsByProduct.find(iter->first);
					if(findMovieRes == valsByProduct.end()) continue;
					findRes = valsByUser[userId].find(iter->first);
					if(findRes != valsByUser[userId].end()) continue;

					desVec = getvectorDesviacion(valsByProduct, valsByUser, userId, iter->first);
					res = slopeOne(desVec, valsByProduct, valsByUser, userId, iter->first);
					valoraciones.push_back(make_tuple(iter->first,res));
					//cout<<res<<endl;
					if(res >= (ValType) MAX_RATING - 0.000005){
						cout<<"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"<<endl;
						valoracionesRes.push_back(make_tuple(iter->first,res));	
					} 

				}
				if(valoraciones.size() == k){
					for(auto resTuple : valoracionesRes){
						cout<<get<1>(resTuple)<<" -> "<<get<0>(movieVec[get<0>(resTuple)])<<endl;
					}
				}
				else{
					sort(valoraciones.begin(), valoraciones.end(), [](tuple<MovieId,Valoration> a, tuple<MovieId, Valoration> b){
						return get<1>(a) > get<1>(b);
					});
					if(valoraciones.size() > k) valoraciones.erase(valoraciones.begin() + k, valoraciones.end());
					for(auto resTuple : valoraciones){
						cout<<get<1>(resTuple)<<" -> "<<get<0>(movieVec[get<0>(resTuple)])<<endl;
					}
				}
				mytime.end();
				cout<<"Done->";
				mytime.print();
				
				break;
			}
			case 7:{
				int k;
				cout<<"UserId->";
				cin>>userId;
				userId--;
				cout<<"K->";
				cin>>k;
				vector<tuple<MovieId,Valoration>> valoraciones;
				InterRegisterMap::iterator findRes;
				map<MovieId,map<UserId,ValType>>::iterator findMovieRes;
				MovieLensVectorDesviacion desVec;
				ValType res = 0;
				
				thread threads[NUM_THREADS];
				vector<tuple<MovieId,Valoration>> * tRes[NUM_THREADS];
				int h = movieVec.size() / NUM_THREADS;
				map<MovieId, MovieRegister>::iterator ini = movieVec.begin();
				map<MovieId, MovieRegister>::iterator end = ini;
				for(int i = 0; i < NUM_THREADS; i++){
					tRes[i] = new vector<tuple<MovieId,Valoration>>();
					if(i == NUM_THREADS - 1) end = movieVec.end();
					else{
						for(int j = 0; j < h; j++){
							++end;
						}
					}
					threads[i] = thread(getSlope,&valsByUser,&valsByProduct,ini,end,userId,tRes[i]);
					ini = end;
				}

				for(int i = 0; i < NUM_THREADS; i++){
					threads[i].join();
					valoraciones.insert(valoraciones.end(), tRes[i]->begin(), tRes[i]->end());
				}
				sort(valoraciones.begin(), valoraciones.end(), [](tuple<MovieId,Valoration> a, tuple<MovieId, Valoration> b){
					return get<1>(a) > get<1>(b);
				});
				if(valoraciones.size() > k) valoraciones.erase(valoraciones.begin() + k, valoraciones.end());
				for(auto resTuple : valoraciones){
					cout<<get<1>(resTuple)<<" -> "<<get<0>(movieVec[get<0>(resTuple)])<<endl;
				}
				break;
			}
		}

	}
}