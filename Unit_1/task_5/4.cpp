#include <iostream>
#include <string>

int main()
{
  int a;
  std::cout <<"Input number: ";
  std::cin >> a;
  if(std::cin.fail()) {
      std::cout << "Invalid value. Please, input integer only"<< std::endl;
      return 0;
  }
  if (a % 2 == 0) {
        std::cout << "Number is even" << std::endl;
    }
  else {
      std::cout << "Number is odd" << std::endl;
  }
    return 0;
}