#include <iostream>
#include <string>

int main()
{
  for (int i = 1; i <= 10; i++)
  {
      int sq = std::pow(i, 2);
      std::cout << sq << std::endl;
  }
}