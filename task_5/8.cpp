#include <iostream>
#include <string>

const int N = 5;

void fill_array(float arr[N][N])
{
	for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < N; j++)
		{
			arr[i][j] = rand()%30+30;
		}
	}
}

float find_max(float arr[N][N])
{
    float max_val = -1;
    for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < N; j++)
		{
			if (arr[i][j] > max_val){
			    max_val = arr[i][j];
			}
		}
	}
	return max_val;
}

float find_min(float arr[N][N])
{
    float min_val = 61;
    for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < N; j++)
		{
			if (arr[i][j] < min_val){
			    min_val = arr[i][j];
			}
		}
	}
	return min_val;
}

int main()
{
	float arr[N][N];
	fill_array(arr);
    float max_val = find_max(arr);
    float min_val = find_min(arr);
    std::cout << max_val << std::endl;
    std::cout << min_val << std::endl;
}
