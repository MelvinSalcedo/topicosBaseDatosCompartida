#include <string>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <sstream>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <string>
#include <ctime>
#include <set>
#include <cmath>
#include <vector>

using namespace std;
typedef int value;
typedef unordered_map<value,value> hashmap;


class ListItem{
    public:
        ListItem();
        void add_rating(string user, string item, int rating);
        int get_userid(string user);
        string get_username(int userid);
        void add_user(string user);
        unordered_map<int,int> get_itemratings(string item);
        int size();
        int n_users();
        void write_csv(string file);
        double get_user_avg(string user);
        double get_item_avg(string item);
        double general_avg();
        void compute_general_avgs();
        double get_genavg_item();
        double get_genavg_user();
        set<string> get_user_list(string user);
        vector<string> get_items();
    private:
        unordered_map<string,int> UserId;
        unordered_map<int,string> IdUser;
        double general_avg_item;
        double general_avg_user;
        int n_id;
        pair<double,double> gen_avg;
        unordered_map<string, unordered_map<int, int>> ItemRatings;
        map<string, pair<double,int>> UserAvg;
        map<string, pair<double,int>> ItemAvg;
        map<string, set<string>> UserList;
};

class FileReader{
    public:
        FileReader(string path);
        ListItem read_csv(string file);
    private:
        string path;
};

FileReader::FileReader(string path){
    this->path = path;
}

ListItem FileReader::read_csv(string file){
    string line;
    string complete_path = file;
    ifstream f(complete_path);
    ListItem list;
    if (f){
        getline(f,line);
        while (getline(f,line)){
            value colon = line.find(":");
            string user = line.substr(0,colon);
            value comma = line.find(",");
            string item = line.substr(colon+1,(comma-colon-1));
            line = line.substr(comma+1,(line.find(",",comma+1) - comma - 1));
            value rating = stoi(line);
            list.add_rating(user,item,rating);
        }
        f.close();
    }
    list.compute_general_avgs();
    return list;
}


class Recommender{
    public:
        Recommender(string filename);
        void predict_all(ListItem &first_list);
        double adjusted_cosine(ListItem &list, string item1, string item2);
        double regular_cosine(hashmap &ratings1, double norm1, hashmap &ratings2, double norm2);
        double calculate_norm(hashmap &rat);
    private:
        string file_to_predict;
        unordered_map<string, unordered_map<string, double> > similars; //maps each item to a list
        unordered_map<string, double> norms;
};


ListItem::ListItem(){
    this->n_id = 0;
    pair<double,double> a(0,0);
    gen_avg = a;
}

int ListItem::get_userid(string user){
    unordered_map<string,int>::iterator it = UserId.find(user);
    if (it != UserId.end()){
        return it->second;
    }
    return -1;
}

void ListItem::add_user(string user){
    if (get_userid(user) < 0){
        UserId.insert({user,n_id});
        IdUser.insert({n_id,user});
        n_id++;
    }
}

void ListItem::compute_general_avgs(){
    double a = 0;
    map<string, pair<double,int> >::iterator it;
    for (it = ItemAvg.begin(); it!=ItemAvg.end(); it++){
        a += it->second.first/((double) it->second.second);
    }
    this->general_avg_item = a/((double) ItemAvg.size());
    a = 0;
    for (it = UserAvg.begin(); it!=UserAvg.end(); it++){
        a += it->second.first/((double) it->second.second);
    }
    this->general_avg_user = a/((double) UserAvg.size());
}

double ListItem::get_genavg_item(){
    return this->general_avg_item;
}

double ListItem::get_genavg_user(){
    return this->general_avg_user;
}

void ListItem::add_rating(string user, string item, int rating){
    gen_avg.first += (double) rating;
    gen_avg.second++;
    unordered_map<string, unordered_map<int,int> >::iterator it = ItemRatings.find(item);
    if (it == ItemRatings.end()){
        unordered_map<int, int> ratings;
        ItemRatings.insert({item,ratings});
        it = ItemRatings.find(item);
        ItemAvg.insert({item, {0,0}});
    }
    int user_id = get_userid(user);
    if (user_id < 0){
        set<string> empty;
        UserList.insert({user,empty});
        add_user(user);
        user_id = get_userid(user);
        UserAvg.insert({user, {0,0}});
    }
    it->second.insert({user_id,rating});
    map<string, set<string> >::iterator userlist_it = UserList.find(user);
    userlist_it->second.insert(item);

    map<string, pair<double,int> >::iterator user_it = UserAvg.find(user);
    user_it->second.first += rating;
    user_it->second.second += 1;
    map<string, pair<double,int> >::iterator item_it = ItemAvg.find(item);
    item_it->second.first += rating;
    item_it->second.second += 1;
}

string ListItem::get_username(int userid){
    unordered_map<int,string>::iterator it = IdUser.find(userid);
    return it->second;
}

set<string> ListItem::get_user_list(string user){
    map<string, set<string> >::iterator it = UserList.find(user);
    if (it == UserList.end()){
        set<string> empty;
        return empty;
    }
    return it->second;
}

vector<string> ListItem::get_items(){
    vector<string> items;
    for (map<string, pair<double,int> >::iterator it = ItemAvg.begin(); it != ItemAvg.end(); ++it){
        items.push_back(it->first);
    }
    return items;
}

double ListItem::get_user_avg(string user){
    map<string, pair<double,int> >::iterator it = UserAvg.find(user);
    if (it == UserAvg.end()){
        return -1;
    }
    double average = it->second.first/((double) it->second.second);
    return average;
}

double ListItem::general_avg(){
    return (gen_avg.first/gen_avg.second);
}

double ListItem::get_item_avg(string item){
    map<string, pair<double,int> >::iterator it = ItemAvg.find(item);
    if (it == ItemAvg.end()){
        return -1;
    }
    double average = it->second.first/((double) it->second.second);
    return average;
}

unordered_map<int,int> ListItem::get_itemratings(string item){
    unordered_map<string, unordered_map<int,int> >::iterator it = ItemRatings.find(item);
    if (it == ItemRatings.end()){
        unordered_map<int,int> empty;
        return empty;
    }
    return it->second;
}

int ListItem::size(){
    return ItemRatings.size();
}

int ListItem::n_users(){
    return UserId.size();
}

void ListItem::write_csv(string file){
    ofstream f (file);
    f << "UserId:ItemId,Prediction\n";
    unordered_map<string, unordered_map<int, int> >::iterator it;
    for (it = ItemRatings.begin(); it != ItemRatings.end(); ++it){
        string item = it->first;
        unordered_map<int,int> user_list = it->second;
        for (unordered_map<int,int>::iterator user_it = user_list.begin(); user_it != user_list.end(); ++user_it){
            int user_id = user_it->first;
            int rating = user_it->second;
            unordered_map<int,string>::iterator find_user = IdUser.find(user_id);
            string user = find_user->second;
            f << user << ":" << item << "," << rating << "\n";
        }
    }
}

Recommender::Recommender(string filename){
    this->file_to_predict = filename;
}

double Recommender::calculate_norm(hashmap &rat){
    double norm = 0;
    for (hashmap::iterator it = rat.begin(); it != rat.end(); it++){
        norm += ((double) it->second)*((double) it->second);
    }
    return sqrt(norm);
}

double Recommender::adjusted_cosine(ListItem &list, string item1, string item2){
    hashmap ratings1 = list.get_itemratings(item1);
    hashmap ratings2 = list.get_itemratings(item2);
    hashmap unite = ratings1;
    unite.insert(ratings2.begin(),ratings2.end());
    double num = 0;

    if (unite.size() != 0){
        double general_average = list.general_avg();
        double den1 = 0;
        double den2 = 0;
        double fill1 = list.get_item_avg(item1);
        if (fill1 < 0)  fill1 = general_average;
        double fill2 = list.get_item_avg(item2);
        if (fill2 < 0)  fill2 = general_average;
        for (hashmap::iterator it = unite.begin(); it != unite.end(); ++it){
            string usr = list.get_username(it->first);
            double avg = list.get_user_avg(usr);
            hashmap::iterator it_item1 = ratings1.find(it->first);
            double r1=fill1;
            double r2=fill2;
            if (it_item1 != ratings1.end()){
                r1 = (double) it_item1->second;
            }

            hashmap::iterator it_item2 = ratings2.find(it->first);
            if (it_item2 != ratings2.end()){
                r2 = (double) it_item2->second;
            }
            den1 += (r1 - avg)*(r1 - avg);
            den2 += (r2 - avg)*(r2 - avg);
            num += (r1-avg)*(r2-avg);
        }
        num = num/(sqrt(den1)*sqrt(den2));
        if (den1 == 0 || den2 == 0){
            num = 0;
        }
    }
    return num;
}

double Recommender::regular_cosine(hashmap &smaller, double norm1, hashmap &bigger, double norm2){
    double num = 0;

    if (smaller.size() != 0){
       for (hashmap::iterator it = smaller.begin(); it != smaller.end(); ++it){
           hashmap::iterator it_big = bigger.find(it->first);
           if (it_big != bigger.end()){
               num += ((double) it->second)*((double) it_big->second);
           }
       }
       num = num/(norm1*norm2);
       if (norm1 == 0 || norm2 == 0){
           num = 0;
       }
    }
    return num;
}

void Recommender::predict_all(ListItem &first_list){
    ifstream f(file_to_predict);
    string line;
    string content ((std::istreambuf_iterator<char>(f)),
                                   (std::istreambuf_iterator<char>()));
    istringstream iss(content);
    value k = 30;
    ostringstream oss;
    getline(iss,line);
    oss << "UserId:ItemId,Prediction\n";
    if (f){
        while(getline(iss,line)){
            value colon = line.find(":");
            string user = line.substr(0,8);
            string item = line.substr(colon+1,8);
		    hashmap ratings1 = first_list.get_itemratings(item);
		    unordered_map<string,double>::iterator norm_it = norms.find(item);
            double norm1 = 0;
            if (norm_it == norms.end()){
                norm1 = calculate_norm(ratings1);
                norms.insert({item,norm1});
            }
            else{
                norm1 = norm_it->second;
            }

            unordered_map<string, unordered_map<string,double> >::iterator it_sim = similars.find(item);
            if (it_sim == similars.end()){
                unordered_map<string,double> empty;
                similars.insert({item,empty});
                it_sim = similars.find(item);
            }
            set<string> this_users_list = first_list.get_user_list(user);
            map<double,string,greater<double> > similarities;
            for (set<string>::iterator it = this_users_list.begin(); it != this_users_list.end(); it++){
                unordered_map<string, unordered_map<string,double> >::iterator find_item = similars.find(*it);
                if (find_item == similars.end()){
                    unordered_map<string,double> empty;
                    similars.insert({*it,empty});
                    find_item = similars.find(*it);
                }
                unordered_map<string,double>::iterator this_similars = it_sim->second.find(*it);
                double sim = 0;
                if (this_similars != it_sim->second.end()){
                    sim = this_similars->second;
                }
                else{
			        hashmap ratings2 = first_list.get_itemratings(*it);
			        norm_it = norms.find(*it);
                    double norm2 = 0;
                    if (norm_it == norms.end()){
                        norm2 = calculate_norm(ratings2);
                        norms.insert({*it,norm2});
                    }
                    else{
                        norm2 = norm_it->second;
                    }
                    if (norm1 > 0 && norm2 > 0){
                        if (ratings1.size() < ratings2.size()){
                            sim = regular_cosine(ratings1, norm1, ratings2, norm2);
                        }
  	                    else{
                            sim = regular_cosine(ratings2, norm1, ratings1, norm2);
                        }
                    }
                    it_sim->second.insert({*it,sim});
                    find_item->second.insert({item,sim});
                }
                // pair<double,string> par(sim,*it);
                similarities.insert({sim,*it});
            }
            // sort(similarities.rbegin(),similarities.rend());
            // if (similarities.size() > k){
            //     vector<pair<double,string> > newvec(similarities.begin(),(similarities.begin() + k));
            //     similarities = newvec;
            // }
            double num = 0, den = 0;
            value count = 0;
            map<double, string>::iterator it = similarities.begin();
            //for (map<double, string>::iterator it = similarities.begin(); it != similarities.end(); ++it){
            while((count < k) && (it != similarities.end())){
                value user_id = first_list.get_userid(user);
                hashmap itemratings = first_list.get_itemratings(it->second);
                hashmap::iterator item_it = itemratings.find(user_id);
                double rat = itemratings.find(user_id)->second;
                double avg = first_list.get_item_avg(it->second);
                //cout << "user avg: " << avg  << " sim: " << it->first << " Rating: " << rat << " user_id: " << user_id << "\n";
                num += (((double) rat) - avg)* it->first;
                den += abs(it->first);
                count++;
                it++;
            }
            double this_avg = first_list.get_item_avg(item);
            if (this_avg < 0){
                this_avg = first_list.get_user_avg(user);
                if (this_avg < 0){
                    this_avg = first_list.get_genavg_item();//first_list.general_avg();
                }
            }

            double prediction = this_avg;
            if (den > 0){
                prediction += (num/den);
            }
            if (prediction > 10){
                prediction = 10;
            }
            else if (prediction < 0){
                prediction = 0;
            }
            //cout << this_avg << "  " << num << "  " << den << "  " << prediction << "\n";
            //final_predictions.add_rating(user,item,prediction);
            oss << user << ":" << item << "," << prediction << "\n";
        }
        //ofile.close();
        //final_predictions.write_csv("predictions.csv");
        f.close();
    }
    cout << oss.str();
    // ListItem novo;
    // return novo;
}

int main(int argc, char* argv[]){
    string ratings(argv[1]);
    string targets(argv[2]);
    FileReader fm("");
    time_t t1,t2;
    time(&t1);
    ListItem in_list = fm.read_csv(ratings);
    Recommender p(targets);
    p.predict_all(in_list);
    time(&t2);
    double secs = difftime(t1,t2);
    cout << "Time elapsed: " << secs << "\n";
    return 0;
}