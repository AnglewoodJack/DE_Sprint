"""
Умножить два бинарных числа в формате строк

На вход подаются две строки X1 и X2, содержащие бинарные числа.
Необходимо вывести их бинарное произведение в формате строки.
"""


def bin_multiply(bin_num_1: str, bin_num_2: str) -> str:
	return bin(int(bin_num_1, 2) * int(bin_num_2, 2))[2:]


if __name__ == "__main__":
	print("Multiplication of two binary numbers:")
	b1 = input("Input first binary number = ")
	b2 = input("Input second binary number = ")
	print(bin_multiply(bin_num_1=b1, bin_num_2=b2))
