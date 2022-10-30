#include <iostream>
#include <string>
#include <ctime>

const int N = 5;
const int ch_MAX = 3;
int unique_nums[5] = {0, 0, 0, 0, 0};

struct time {
    int hour;
    int minute;
};

struct train {
  std::string destination;
  int train_id;
  struct time depart_time;
};

std::string get_destination()
{
    int ch = 2;
    char alpha[ch_MAX] = { 'a', 'b', 'c'};
    std::string result = "";
    for (int i = 0; i<ch; i++)
        result = result + alpha[rand() % ch_MAX];

    return result;
}

int get_train_id(int idx)
{
    int min = 1;
    int max = 100;
    int num;
    bool unique = true;
    while (true) {
        num = rand()%(max-min + 1) + min;
        if (idx == 0) {
            unique_nums[idx] = num;
            return num;
        }
        for (int k=0; k<idx+1; k++) {
    	    if (num == unique_nums[k]) {
    	        break;
    	    }
    	    if (k == idx) {
    	        unique_nums[idx] = num;
    	        return num;
    	    }
        }
    }
    unique_nums[idx] = num;
    return num;
}

struct time get_depart_time()
{
    struct time time_gen;
    int min_h = 1;
    int max_h = 23;
    int min_m = 1;
    int max_m = 59;
	time_gen.hour = rand()%(max_h-min_h + 1) + min_h;
	time_gen.minute = rand()%(max_m-min_m + 1) + min_m;
	return time_gen;
}

int main()
{
    // создаем массив
	train trains[5];
	for (int i = 0; i<5; i++){
	    trains[i].destination = get_destination();
	    trains[i].train_id = get_train_id(i);
	    trains[i].depart_time = get_depart_time();
	}
	// фильтруем по номеру поеду
	train filtered[5];
	for (int j=0; j<5; j++){
	    int min_filter_train_num = 101;
	    int min_idx = -1;
    	for (int i = 0; i<5; i++){
    	    if (min_filter_train_num > trains[i].train_id) {
    	        min_filter_train_num = trains[i].train_id;
    	        min_idx = i;
    	    }
	    }
	    filtered[j] = trains[min_idx];
	    trains[min_idx].train_id = 10000;
	}
	std::cout << "IDs in the incresing order" <<std::endl;
	for (int i=0; i<5; i++){
	    std::cout << filtered[i].train_id <<std::endl;
	    }
 	// выводим
 	int train_num_find;
 	int train_idx;
 	std::cout << "Enter the train number" <<std::endl;
 	std::cin >> train_num_find;
 	for (int i = 0; i < N; i++)
	{
		if (filtered[i].train_id == train_num_find){
		    std::cout << "The train destination is " << filtered[i].destination;
		    break;
		}
		if (i == N-1 and filtered[i].train_id != train_num_find){
		std::cout << "no such train" << std::endl;
		}
	}

 	// сортировка по месту
 	for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++) {
            if (filtered[i].destination < filtered[j].destination && filtered[i].train_id== filtered[j].train_id) {
                train tmp = filtered[i];
                filtered[i] = filtered[j];
                filtered[j] = tmp;
            }
        }
    }
}