"""
Валидность скобок

Дана строка X, состоящая только из символов “{“, “}”, “[“, “]”, “(“, “)”. Программа должна вывести True, в том случае
если все открытые скобки закрыты. Например: “[()]{}”, все открытые скобки закрыты закрывающимися скобками, потому вывод
будет True. В случае же, если строка будет похожа на: “{{{}”, то вывод будет False, т.к. не все открытые скобки закрыты.
"""


def check_brackets(bracket_string: str) -> bool:
    for _ in bracket_string:
        for reference in ["{}", "[]", "()"]:
            bracket_string = bracket_string.replace(reference, "")
    if len(bracket_string) == 0:
        return True
    return False


if __name__ == "__main__":
    sample = input("Input bracket string for check: ")
    print(check_brackets(sample))
