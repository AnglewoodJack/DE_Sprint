#include <iostream>
#include <string>

int main()
{
  int max_val = -1;
  int arr_size;
  std::cout << "Input array lingth: ";
  std::cin >> arr_size;
  int arr[arr_size];
  std::cout << "Input array elemetns";
  for (int i=0; i<arr_size; i++){
      std::cout << "Input element №" << i 
      std::cin >> arr[i];
  }
  int i = 0;
  do{
      if (arr[i] > max_val){
          max_val = arr[i];
          }
      i++;
      }
  while (i < arr_size);
  std::cout << "Max value is " << max_val;
}