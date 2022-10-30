#include <iostream>
#include <string>

int main()
{
  std::string name;
  int a, b, c, discriminant, x1, x2, realPart, imaginaryPart;
  std::cout <<"Inout coefficients a, b and c: "<< std::endl ;
  std::cin >> a >> b >> c;
  if(std::cin.fail()) {
      std::cout << "Invalid value. Please, paste integer only"<< std::endl;
  }
  if (discriminant > 0) {
        x1 = (-b + std::sqrt(discriminant)) / (2*a);
        x2 = (-b - std::sqrt(discriminant)) / (2*a);
        std::cout << "Roots are real and different." << std::endl;
        std::cout << "x1 = " << x1 << std::endl;
        std::cout << "x2 = " << x2 << std::endl;
    }
  else if (discriminant == 0) {
        std::cout << "Roots are real and same.";
        x1 = -b/(2*a);
        std::cout << "x1 = x2 =" << x1;
    }
else {
        realPart = -b/(2*a);
        imaginaryPart = std::sqrt(-discriminant)/(2*a);
        std::cout << "Roots are complex and different."<< std::endl;
        std::cout << "x1 = " << realPart << "+" << imaginaryPart << "i"<< std::endl;
        std::cout << "x2 = " << realPart << "-" << imaginaryPart << "i"<< std::endl;
    }
    return 0;
}