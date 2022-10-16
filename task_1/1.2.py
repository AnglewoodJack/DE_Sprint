"""
Палиндром строки

Дана строка X, возвращайте True, если X является палиндромом.
Строка является палиндромом тогда, когда она читается одинаково как в обратном, так и в прямом направлении.
Например, является “taco cat” является палиндромом, в то время как “black cat” не является.
В данной задаче пробелы не учитываются.
Гарантируется, что исходная строка может содержать символы только нижнего регистра.
"""


def check_palindrome(string_to_check: str) -> bool:
	"""
	Checks if an input phrase is a palindrome.
	:param string_to_check:
	:return: True if phrase is a palindrome, False otherwise.
	"""
	string_to_check = ''.join(string_to_check.split())
	if string_to_check == string_to_check[::-1]:
		return True
	return False


if __name__ == "__main__":
	x = input("Input string: ")
	print(check_palindrome(x))
