"""
Перевод арабского числа в римское

Дано целое положительное число X, необходимо вывести вариант этого числа в римской системе счисления в формате строки.
Римские числа записываются от наибольшего числа к наименьшему слева направо.
Однако число 4 не является “IIII”. Вместо этого число 4 записывается, как “IV”. Т.к. 1 стоит перед 5, мы вычитаем 1,
делая 4. Тот же принцип применим к числу 9, которое записывается как “IX”.

Случаи, когда используется вычитание:
I может быть помещен перед V и X, чтобы сделать 4 и 9.
X может быть помещен перед L и C, чтобы составить 40 и 90.
C может быть помещен перед D и M, чтобы составить 400 и 900.
"""


def arabic2roman(arabic_number: int) -> str:
    """
    Convert arabic number to roman.
    :param arabic_number: arabic number.
    :return: roman number.
    """
    num_map = [
        (1000, 'M'),
        (900, 'CM'),
        (500, 'D'),
        (400, 'CD'),
        (100, 'C'),
        (90, 'XC'),
        (50, 'L'),
        (40, 'XL'),
        (10, 'X'),
        (9, 'IX'),
        (5, 'V'),
        (4, 'IV'),
        (1, 'I'),
    ]

    roman = ''
    while arabic_number > 0:
        for i, r in num_map:
            while arabic_number >= i:
                roman += r
                arabic_number -= i
    return roman


if __name__ == "__main__":
    num = int(input("Integer to convert: "))
    print(arabic2roman(arabic_number=num))
