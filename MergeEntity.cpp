#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string>
#include <map>

using namespace std;

string entity, name;

void split(string line){
	int i = 0, tmp;
	for (; i < line.length(); i++){
		if (line[i] == '\t'){
			entity = line.substr(0, i);
			tmp = i+1;
			break;
		}
	}
	name = line.substr(tmp, line.length()-tmp);
}

int main(){
	ifstream name_in("/media/kevin/D/database/entity_name.txt");

	int nline = 0;
	string line;

	map<string, string> name_map;

	while (getline(name_in, line)){
		split(line);
		name_map[entity] = name;
	
		nline ++;
		if (nline % 1000000 == 0){
			cout << nline << endl;
		}

	}

	cout << nline << '\t' << name_map.size() << endl;
	
	ifstream entity_in("/media/kevin/D/database/entity.dat");
	ofstream entity_out("/media/kevin/D/database/entity");

	nline = 0;
	string buffer = "";

	while (getline(entity_in, line)){
		buffer += line;
		if (name_map.find(line) == name_map.end()){
			buffer += '\n';
		} else {
			buffer += '\t' + name_map.find(line)->second + '\n';
		}

		nline ++;
		if (nline % 1000 == 0){
			entity_out << buffer;
			buffer = "";
		}

		if (nline % 1000000 == 0){
			cout << nline << endl;
		}
	}

	if (nline % 1000){
		entity_out << buffer;
	}

}