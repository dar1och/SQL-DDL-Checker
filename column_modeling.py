import re
from typing import List

# Список зарезервированных слов ANSI SQL
RESERVED_WORDS = {
    "SELECT", "FROM", "WHERE", "TABLE", "INSERT", "UPDATE", "DELETE",
    "ORDER", "GROUP", "BY", "HAVING", "JOIN", "INNER", "OUTER", "LEFT", "RIGHT"
}

def find_conflicting_column_types_with_lines(sql_text):
    # словарь для хранения информации о типах колонок по имени
    column_types = {}  # {имя_колонки: {тип_данных: [(имя_таблицы, номер_строки)]}}
    errors = []

    table_name = None
    lines = sql_text.split("\n")

    # шаблоны для поиска начала таблицы и определения столбцов
    create_table_pattern = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    column_pattern = re.compile(r"(\w+)\s+(\w+(?:\(\d+\))?)", re.IGNORECASE)

    for i, line in enumerate(lines, start=1):
        # проверка начала новой таблицы
        table_match = create_table_pattern.search(line)
        if table_match:
            table_name = table_match.group(1)
            continue

        if table_name:
            # поиск определения столбца
            column_match = column_pattern.match(line.strip())
            if column_match:
                column_name = column_match.group(1).lower()
                column_type = column_match.group(2).lower()

                # добавление информации о колонке
                if column_name not in column_types:
                    column_types[column_name] = {}

                if column_type not in column_types[column_name]:
                    column_types[column_name][column_type] = []

                column_types[column_name][column_type].append((table_name, i))

    # проверка на противоречивые определения типов
    for column_name, type_definitions in column_types.items():
        if len(type_definitions) > 1:
            error_msg = f"Ошибка: Колонка '{column_name}' имеет противоречивые определения типов данных:\n"
            for column_type, occurrences in type_definitions.items():
                for table, line in occurrences:
                    error_msg += f" - В таблице '{table}' (строка {line}): тип {column_type}\n"
            error_msg += "Рекомендация: Проверьте бизнес-правила и при необходимости приведите колонки к одному типу данных."
            errors.append(error_msg)

    return errors

def find_conflicting_default_values(sql_text: str) -> List[str]:
    # Компилированные регулярные выражения для поиска определения таблицы и столбца со значением DEFAULT
    create_table_pattern = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    # Здесь группа (3) захватывает значение по умолчанию, включающее вариант с кавычками или без них.
    column_pattern = re.compile(
        r"(\w+)\s+(\w+(?:\(\d+\))?)\s+DEFAULT\s+((?:'[^']*')|\d+(?:\.\d+)?|\w+)",
        re.IGNORECASE
    )

    errors: List[str] = []
    table_name: str | None = None

    # Используем splitlines() для корректной обработки строк независимо от символов перевода строки
    lines = sql_text.splitlines()

    for line_no, line in enumerate(lines, start=1):
        # Ищем начало создания таблицы
        table_match = create_table_pattern.search(line)
        if table_match:
            table_name = table_match.group(1)
            continue  # Переходим к следующим строкам, где определены столбцы

        # Если таблица уже найдена, ищем определения столбцов со значением DEFAULT
        if table_name:
            column_match = column_pattern.search(line.strip())
            if column_match:
                column_name = column_match.group(1).lower()
                data_type_raw = column_match.group(2).lower()
                default_raw = column_match.group(3).strip()  # не удаляем кавычки сразу

                # Извлекаем основной тип данных и (опционально) размер (например, varchar(50))
                data_type_parts = re.match(r"(\w+)(?:\((\d+)\))?", data_type_raw)
                if not data_type_parts:
                    continue  # Если не удалось распознать тип данных, переходим к следующей строке

                data_type = data_type_parts.group(1)
                data_size = int(data_type_parts.group(2)) if data_type_parts.group(2) else None

                # Проверка для текстовых типов
                if data_type in {"varchar", "varchar2", "char"}:
                    # Если значение по умолчанию не заключено в одинарные кавычки – ошибка
                    if not (default_raw.startswith("'") and default_raw.endswith("'")):
                        errors.append(
                            f"Ошибка: в таблице '{table_name}' столбец '{column_name}' имеет значение по умолчанию "
                            f"{default_raw} без кавычек, что недопустимо для типа {data_type} (строка {line_no})"
                        )
                    else:
                        # Убираем кавычки для проверки длины по типу
                        default_value = default_raw.strip("'")
                        if data_size and len(default_value) > data_size:
                            errors.append(
                                f"Ошибка: в таблице '{table_name}' столбец '{column_name}' имеет значение по умолчанию "
                                f"'{default_value}', длина которого превышает размер {data_size} для типа {data_type} (строка {line_no})"
                            )

                # Проверка для числовых типов
                elif data_type in {"int", "integer", "number"}:
                    try:
                        # Если значение взято в кавычки, пробуем его преобразовать после удаления кавычек
                        test_value = default_raw.strip("'") if (default_raw.startswith("'") and default_raw.endswith("'")) else default_raw
                        float(test_value)
                    except ValueError:
                        errors.append(
                            f"Ошибка: в таблице '{table_name}' столбец '{column_name}' имеет значение по умолчанию "
                            f"{default_raw}, которое не может быть преобразовано в число для типа {data_type} (строка {line_no})"
                        )

    return errors

def find_reserved_word_columns(sql_text):
    errors = []
    reported = set()  # Набор для хранения уникальных (table, column)
    table_name = None  # Текущая таблица
    lines = sql_text.split("\n")

    # Шаблон для поиска начала определения таблицы.
    create_table_pattern = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    # Шаблон для поиска определения колонки (предполагается, что имя колонки идет первым)
    column_pattern = re.compile(r"^\s*(\w+)\s+[\w()]+", re.IGNORECASE)

    for i, line in enumerate(lines, start=1):
        # Ищем начало определения таблицы.
        table_match = create_table_pattern.search(line)
        if table_match:
            table_name = table_match.group(1)
            continue

        # Если встречается закрывающая скобка, предполагаем конец определения таблицы.
        if table_name and line.strip().startswith(")"):
            table_name = None
            continue

        # Если находим определение колонки и таблица активна.
        if table_name:
            col_match = column_pattern.match(line)
            if col_match:
                col_name = col_match.group(1)
                # Если колонка уже была зарегистрирована в рамках этой таблицы, пропускаем.
                if (table_name, col_name.upper()) in reported:
                    continue
                if col_name.upper() in RESERVED_WORDS:
                    error_msg = (f"Ошибка: Колонка '{col_name}' в таблице '{table_name}' (строка {i}) использует "
                                 f"зарезервированное слово ANSI SQL.\nРекомендация: переименуйте колонку, чтобы избежать конфликтов при генерации схемы СУБД.")
                    errors.append(error_msg)
                    reported.add((table_name, col_name.upper()))
    return errors