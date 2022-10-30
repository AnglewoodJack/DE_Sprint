#include <iostream>
#include <string>

const int N = 5;
const int ch_MAX = 26;

void fill_array(int arr[N][N])
{
    int min = 2;
    int max = 5;
	for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < N; j++)
		{
			arr[i][j] = rand()%(max-min + 1) + min;
		}
	}
}

std::string get_name()
{
    int ch = 6;
    char alpha[ch_MAX] = { 'a', 'b', 'c', 'd', 'e', 'f', 'g',
                          'h', 'i', 'j', 'k', 'l', 'm', 'n',
                          'o', 'p', 'q', 'r', 's', 't', 'u',
                          'v', 'w', 'x', 'y', 'z' };
    std::string result = "";
    for (int i = 0; i<ch; i++)
        result = result + alpha[rand() % ch_MAX];

    return result;
}

int get_group_id()
{
    int min = 1;
    int max = 100;
	return rand()%(max-min + 1) + min;
}

int find_mean(int arr[N][N])
{
    int sum = 0;
    for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < N; j++)
		{
			sum += arr[i][j];
		}
	}
	return sum/25;
}

bool check_if_4_and_5(int filtered[N][N])
{
    for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < N; j++)
		{
			if (filtered[i][j] != 4 && filtered[i][j] != 5){
			    return false;
			}
		}
	}
	return true;
}

struct student {
  std::string name;
  int group_id;
  int marks[N][N];
};

int main()
{
    // создаем массив
	student students[10];
	for (int i = 0; i<10; i++){
	    students[i].name = get_name();
	    students[i].group_id = get_group_id();
	    fill_array(students[i].marks);
	}
	// фильтруем
	student filtered[10];
	int idx_filtered = 0;
	for  (int mark_now=2; mark_now<=5; mark_now++){
    	for (int i = 0; i<10; i++){
    	    int mean_now = find_mean(students[i].marks);
    	    if (mean_now == mark_now) {
    	        filtered[idx_filtered] = students[i];
    	        idx_filtered ++;
    	    }
    	}
	}
	// выводим
	for (int i=0; i<10; i++){
	    if (check_if_4_and_5(filtered[i].marks)){
	        std::cout << filtered[i].name <<std::endl;
	    }
	    else{
	        std::cout << "This student has not only 4 and 5" << std::endl;
	    }
	}
}
