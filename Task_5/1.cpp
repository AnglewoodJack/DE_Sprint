#include <iostream>
using namespace std;

int main()
{
  int first_num;
  int second_num;
  std::cout << "Input first number: ";
  std::cin >> first_num;
  std::cout << "Input second number: ";
  std::cin >> second_num;
  if(first_num > second_num){
      std::cout << first_num << " is greater that " << second_num << "\n";
  }
  else if(second_num > first_num){
      std::cout << second_num << " is greater that " << first_num << "\n";
  }
  else{
      std::cout << "Numbers are equal" << "\n";
  }
}