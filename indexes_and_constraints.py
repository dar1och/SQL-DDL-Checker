import re
def find_null_in_primary_keys(sql_text):
    errors = []
    lines = sql_text.split("\n")
    table_name = None
    in_table = False
    current_columns = {}
    line_number_map = {}

    # Шаблоны для поиска
    table_creation_pattern = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    column_def_pattern = re.compile(r"\s*(\w+)\s+([\w()]+(?:\s+\w+)*).*", re.IGNORECASE)
    inline_pk_pattern = re.compile(r".*PRIMARY\s+KEY.*", re.IGNORECASE)
    constraint_pk_pattern = re.compile(r".*PRIMARY\s+KEY\s*\(([^)]+)\)", re.IGNORECASE)

    for i, line in enumerate(lines, start=1):
        stripped = line.strip()

        # Поиск начала оператора CREATE TABLE
        match_table_creation = table_creation_pattern.match(stripped)
        if match_table_creation:
            table_name = match_table_creation.group(1)
            in_table = True
            current_columns = {}
            line_number_map = {}
            continue

        if in_table:
            # Если встретили закрывающую скобку, завершаем обработку таблицы
            if stripped.startswith(")"):
                in_table = False
                continue

            # Если строка начинается с CONSTRAINT, обрабатываем ограничение отдельно
            if stripped.upper().startswith("CONSTRAINT"):
                match_constraint = constraint_pk_pattern.match(stripped)
                if match_constraint:
                    pk_columns = [col.strip() for col in match_constraint.group(1).split(",")]
                    for col in pk_columns:
                        definition = current_columns.get(col)
                        if definition and "NOT NULL" not in definition.upper():
                            errors.append(
                                f"Ошибка: В таблице '{table_name}' колонка '{col}' (строка {line_number_map.get(col, i)}) участвует в PRIMARY KEY,\n"
                                "но не содержит NOT NULL.\nРекомендация: Первичный ключ не должен содержать NULL-значения."
                            )
                continue

            # Обработка описания колонки
            match_column = column_def_pattern.match(stripped)
            if match_column:
                col_name = match_column.group(1)
                definition = stripped
                current_columns[col_name] = definition
                line_number_map[col_name] = i
                # Проверка: inline объявление PRIMARY KEY должно содержать NOT NULL
                if "PRIMARY KEY" in definition.upper() and "NOT NULL" not in definition.upper():
                    errors.append(
                        f"Ошибка: В таблице '{table_name}' колонка '{col_name}' (строка {i}) объявлена как PRIMARY KEY без NOT NULL.\n"
                        "Рекомендация: Убедитесь, что все первичные ключи не допускают значение NULL."
                    )
    return errors
def find_index_anomalies(sql_text):
    """
    Ищет аномалии индексов:
    A) Индекс содержит супернабор первичного ключа
    B) Индекс содержит супернабор уникального индекса
    """
    lines = sql_text.splitlines()

    # 1) Найти все PK и UNIQUE в таблицах
    pk_columns = {}     # { table_name: set(column_names) }
    unique_constraints = {}  # { table_name: [set(columns), ...] }
    current_table = None

    re_create_table = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    re_table_pk = re.compile(r"PRIMARY\s+KEY\s*\(\s*([^)]+)\s*\)", re.IGNORECASE)
    re_table_unique = re.compile(r"UNIQUE\s*\(\s*([^)]+)\s*\)", re.IGNORECASE)
    re_inline_pk = re.compile(r"^\s*(\w+)\s+\w+.*PRIMARY\s+KEY", re.IGNORECASE)
    re_inline_unique = re.compile(r"^\s*(\w+)\s+\w+.*UNIQUE", re.IGNORECASE)

    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m_ct = re_create_table.match(line)
        if m_ct:
            current_table = m_ct.group(1)
            continue
        if current_table and line.startswith(")"):
            current_table = None
            continue
        if current_table:
            m_pk_inline = re_inline_pk.match(line)
            if m_pk_inline:
                pk_columns.setdefault(current_table, set()).add(m_pk_inline.group(1).upper())

            m_pk_table = re_table_pk.search(line)
            if m_pk_table:
                cols = [col.strip().upper() for col in m_pk_table.group(1).split(",")]
                pk_columns.setdefault(current_table, set()).update(cols)

            m_unique_inline = re_inline_unique.match(line)
            if m_unique_inline:
                col = m_unique_inline.group(1).upper()
                unique_constraints.setdefault(current_table, []).append({col})

            m_unique_table = re_table_unique.search(line)
            if m_unique_table:
                cols = {col.strip().upper() for col in m_unique_table.group(1).split(",")}
                unique_constraints.setdefault(current_table, []).append(cols)

    # 2) Найти все индексы
    indexes = {}  # { table_name: [ (index_name, is_unique, [columns]) ] }

    re_index = re.compile(
        r"CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)", re.IGNORECASE)

    for lineno, raw in enumerate(lines, start=1):
        m_idx = re_index.search(raw)
        if m_idx:
            unique_flag, idx_name, table_name, cols_raw = m_idx.groups()
            is_unique = bool(unique_flag)
            cols = [col.strip().upper() for col in cols_raw.split(",")]
            indexes.setdefault(table_name, []).append((idx_name, is_unique, cols))

    # 3) Поиск аномалий
    errors = []

    for table, table_indexes in indexes.items():
        pks = pk_columns.get(table, set())
        uniques = unique_constraints.get(table, [])

        # A) Индекс содержит все PK + что-то ещё
        for idx_name, is_unique, idx_cols in table_indexes:
            if pks and set(pks).issubset(idx_cols) and len(idx_cols) > len(pks):
                errors.append(
                    f"Аномалия индекса в таблице '{table}':\n"
                    f" - Индекс '{idx_name}' включает все столбцы первичного ключа и дополнительные.\n"
                    f"Рекомендация: сделать индекс '{idx_name}' уникальным или пересмотреть состав первичного ключа.\n"
                    f"Столбцы индекса: {', '.join(idx_cols)}\n"
                    f"Столбцы PK: {', '.join(pks)}"
                )

        # B) Индекс содержит все столбцы уникального ограничения + что-то ещё
        for idx_name, is_unique, idx_cols in table_indexes:
            idx_cols_set = set(idx_cols)
            for uniq_cols in uniques:
                if uniq_cols.issubset(idx_cols_set) and len(idx_cols_set) > len(uniq_cols):
                    if not is_unique:
                        errors.append(
                            f"Аномалия индекса в таблице '{table}':\n"
                            f" - Индекс '{idx_name}' включает все столбцы уникального ограничения ({', '.join(uniq_cols)}) и дополнительные.\n"
                            f"Рекомендация: сделать индекс '{idx_name}' уникальным.\n"
                            f"Столбцы индекса: {', '.join(idx_cols)}"
                        )

    return errors

def find_redundant_unique_indexes(sql_text):
    """
    Находит уникальные индексы, которые эквивалентны первичному ключу (дублируют его колонки).
    Такие индексы должны быть удалены или переопределены.
    """
    errors = []
    lines = sql_text.splitlines()

    # 1) Собираем первичные ключи
    pk_columns = {}  # {table: set(columns)}
    current_table = None
    re_create_table = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    re_inline_pk = re.compile(r"^\s*(\w+)\s+\w+.*PRIMARY\s+KEY", re.IGNORECASE)
    re_table_pk = re.compile(r"PRIMARY\s+KEY\s*\(\s*([^)]+?)\s*\)", re.IGNORECASE)

    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m_ct = re_create_table.match(line)
        if m_ct:
            current_table = m_ct.group(1)
            continue
        if current_table and line.startswith(")"):
            current_table = None
            continue
        if current_table:
            # inline PK
            m_pk_inline = re_inline_pk.match(line)
            if m_pk_inline:
                col = m_pk_inline.group(1).upper()
                pk_columns.setdefault(current_table, set()).add(col)
            # table-level PK
            m_pk_table = re_table_pk.search(line)
            if m_pk_table:
                cols = [c.strip().upper() for c in m_pk_table.group(1).split(",")]
                pk_columns.setdefault(current_table, set()).update(cols)

    # 2) Собираем индексы
    indexes = {}  # {table: [(idx_name, is_unique, [cols])]}
    re_index = re.compile(
        r"CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)",
        re.IGNORECASE
    )
    for raw in lines:
        m_idx = re_index.search(raw)
        if m_idx:
            unique_flag, idx_name, table, cols_raw = m_idx.groups()
            is_unique = bool(unique_flag)
            cols = [c.strip().upper() for c in cols_raw.split(",")]
            indexes.setdefault(table, []).append((idx_name, is_unique, cols))

    # 3) Ищем уникальные индексы, дублирующие PK
    for table, idx_list in indexes.items():
        pk_set = pk_columns.get(table)
        if not pk_set:
            continue
        for idx_name, is_unique, cols in idx_list:
            if is_unique and set(c for c in cols) == pk_set:
                errors.append(
                    f"Уникальный индекс '{idx_name}' в таблице '{table}' дублирует первичный ключ.\n"
                    f"Колонки PK: {', '.join(sorted(pk_set))}\n"
                    f"Колонки индекса: {', '.join(cols)}\n"
                    "Рекомендация: удалить или переопределить этот индекс."
                )
    return errors

def find_tables_without_relations(sql_text):
    """
    Находит таблицы, которые не участвуют ни в одном внешнем ключе (ни как parent, ни как child).
    Такие таблицы могут быть 'изолированными' и требуют анализа предметной области.
    """
    errors = []
    lines = sql_text.splitlines()

    # 1) Собираем все имена таблиц и номера строк их создания
    re_create_table = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    table_lines = {}  # {table: line_number}
    for lineno, raw in enumerate(lines, start=1):
        m = re_create_table.match(raw.strip())
        if m:
            table_lines[m.group(1)] = lineno

    # 2) Собираем все внешние ключи (parent и child)
    fkc_tables = set()  # таблицы-дети
    fkp_tables = set()  # таблицы-родители

    # inline REFERENCES
    re_inline_fk = re.compile(r"REFERENCES\s+(\w+)\s*\(", re.IGNORECASE)
    re_column = re.compile(r"^\s*(\w+)\s+", re.IGNORECASE)
    for lineno, raw in enumerate(lines, start=1):
        for m in re_inline_fk.finditer(raw):
            parent = m.group(1)
            # определить child таблицу (поднимаемся вверх по CREATE TABLE)
            j = lineno - 1
            child = None
            while j > 0:
                mm = re_create_table.match(lines[j-1].strip())
                if mm:
                    child = mm.group(1)
                    break
                j -= 1
            if child:
                fkc_tables.add(child)
                fkp_tables.add(parent)

    # table-level FK
    re_table_fk = re.compile(
        r"FOREIGN\s+KEY.*?REFERENCES\s+(\w+)\s*\(", re.IGNORECASE)
    current_table = None
    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m_ct = re_create_table.match(line)
        if m_ct:
            current_table = m_ct.group(1)
            continue
        if current_table:
            mfk = re_table_fk.search(line)
            if mfk:
                parent = mfk.group(1)
                fkc_tables.add(current_table)
                fkp_tables.add(parent)

    # 3) Ищем таблицы без связей
    for table, ln in table_lines.items():
        if table not in fkc_tables and table not in fkp_tables:
            errors.append(
                f"Таблица '{table}' (строка {ln}) не участвует ни в одном внешнем ключе.\n"
                "Рекомендация: проанализируйте предметную область и при необходимости установите связи через FOREIGN KEY."
            )
    return errors

def find_missing_potential_keys(sql_text):
    """
    Находит таблицы, в которых отсутствует потенциальный ключ:
    ни PRIMARY KEY, ни UNIQUE INDEX, ни UNIQUE CONSTRAINT.
    Каждая таблица должна иметь хотя бы одну гарантию уникальности.
    """
    errors = []
    lines = sql_text.splitlines()

    # 1) Собираем все таблицы с номерами строк их создания
    re_create_table = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    table_lines = {}
    for lineno, raw in enumerate(lines, start=1):
        m = re_create_table.match(raw.strip())
        if m:
            table_lines[m.group(1)] = lineno

    # 2) Находим таблицы с PRIMARY KEY или UNIQUE CONSTRAINT внутри CREATE TABLE
    pk_or_unique = set()
    current_table = None
    re_table_pk     = re.compile(r"PRIMARY\s+KEY", re.IGNORECASE)
    re_table_unique = re.compile(r"UNIQUE\s*\(", re.IGNORECASE)

    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m = re_create_table.match(line)
        if m:
            current_table = m.group(1)
            continue
        if current_table and line.startswith(")"):
            current_table = None
            continue
        if current_table:
            # если в строке есть PRIMARY KEY или UNIQUE(...)
            if re_table_pk.search(line) or re_table_unique.search(line):
                pk_or_unique.add(current_table)

    # 3) Находим таблицы с UNIQUE INDEX
    re_unique_index = re.compile(
        r"CREATE\s+UNIQUE\s+INDEX\s+\w+\s+ON\s+(\w+)", re.IGNORECASE)
    for raw in lines:
        m = re_unique_index.search(raw)
        if m:
            pk_or_unique.add(m.group(1))

    # 4) Любая таблица, не попавшая ни в одну из этих групп, – ошибка
    for table, ln in table_lines.items():
        if table not in pk_or_unique:
            errors.append(
                f"Таблица '{table}' (строка {ln}) не имеет потенциального ключа "
                "(ни PRIMARY KEY, ни UNIQUE CONSTRAINT, ни UNIQUE INDEX).\n"
                "Рекомендация: добавьте PRIMARY KEY, UNIQUE CONSTRAINT или UNIQUE INDEX "
                "для гарантии уникальности строк."
            )

    return errors

def find_unnecessary_indexes(sql_text):
    """
    Находит ненужные индексы:
    A) Индексы на колонках, которые через CHECK(...) зафиксированы в одно значение.
    B) Индексы, чьи колонки полностью включены в другой индекс или в первичный ключ.
    """
    errors = []
    lines = sql_text.splitlines()

    # --- 1) Собираем CHECK(column = const) — константные колонки ---
    # inline: после определения колонки
    re_inline_check = re.compile(r"^\s*(\w+)\s+\w+(?:\([\d,]+\))?.*?CHECK\s*\(\s*\1\s*=\s*[^)]+\)", re.IGNORECASE)
    # table-level: после всех колонок
    re_table_check = re.compile(r"CHECK\s*\(\s*(\w+)\s*=\s*[^)]+\)", re.IGNORECASE)

    const_columns = set()  # {(table, column)}
    re_create_table = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    current_table = None

    for lineno, raw in enumerate(lines, start=1):
        line = raw.rstrip()
        m_ct = re_create_table.match(line.strip())
        if m_ct:
            current_table = m_ct.group(1)
            continue
        if current_table and line.strip().startswith(")"):
            current_table = None
            continue
        if current_table:
            # inline CHECK
            m_in = re_inline_check.match(line)
            if m_in:
                const_columns.add((current_table, m_in.group(1).upper(), lineno))
            # table-level CHECK
            for m_tb in re_table_check.finditer(line):
                const_columns.add((current_table, m_tb.group(1).upper(), lineno))

    # --- 2) Собираем PK и все индексы ---
    pk_cols = {}  # {table: set(cols)}
    indexes = {}  # {table: [(idx_name, is_unique, [cols], lineno)]}

    # PK внутри CREATE TABLE
    re_inline_pk = re.compile(r"^\s*(\w+)\s+\w+(?:\([\d,]+\))?.*PRIMARY\s+KEY", re.IGNORECASE)
    re_table_pk = re.compile(r"PRIMARY\s+KEY\s*\(\s*([^)]+)\)", re.IGNORECASE)

    current_table = None
    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m_ct = re_create_table.match(line)
        if m_ct:
            current_table = m_ct.group(1)
            continue
        if current_table and line.startswith(")"):
            current_table = None
            continue
        if current_table:
            m_inpk = re_inline_pk.match(line)
            if m_inpk:
                pk_cols.setdefault(current_table, set()).add(m_inpk.group(1).upper())
            m_tpk = re_table_pk.search(line)
            if m_tpk:
                cols = [c.strip().upper() for c in m_tpk.group(1).split(",")]
                pk_cols.setdefault(current_table, set()).update(cols)

    # Все индексы
    re_index = re.compile(
        r"CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(\s*([^)]+)\)", re.IGNORECASE)
    for lineno, raw in enumerate(lines, start=1):
        m_idx = re_index.search(raw)
        if not m_idx:
            continue
        is_unique = bool(m_idx.group(1))
        idx_name = m_idx.group(2)
        table = m_idx.group(3)
        cols = [c.strip().upper() for c in m_idx.group(4).split(",")]
        indexes.setdefault(table, []).append((idx_name, is_unique, cols, lineno))

    # --- 3A) Индексы на константных колонках ---
    for table, col, ln in const_columns:
        # найдем индексы, где есть этот столбец (единственный или в составе)
        for idx in indexes.get(table, []):
            idx_name, _, cols, idx_ln = idx
            if col in cols:
                errors.append(
                    f"Ненужный индекс '{idx_name}' в таблице '{table}' (строка {idx_ln}):\n"
                    f" - Содержит колонку '{col}', зафиксированную CHECK на одно значение (строка {ln}).\n"
                    "Рекомендация: удалить этот индекс — он неэффективен на константной колонке."
                )

    # --- 3B) Индексы, чьи колонки – супернабор других индексов или PK ---
    for table, idx_list in indexes.items():
        # собираем все «базовые» наборы колонок: PK и уникальные индексы
        base_sets = []
        if table in pk_cols:
            base_sets.append((f"PRIMARY KEY", pk_cols[table]))
        # уникальные индексы
        for idx_name, is_unique, cols, ln in idx_list:
            if is_unique:
                base_sets.append((f"UNIQUE INDEX {idx_name}", set(cols)))

        # теперь для каждого индекса ищем, является ли он супернабором какого-то базового набора
        for idx_name, is_unique, cols, idx_ln in idx_list:
            cols_set = set(cols)
            for base_name, base_cols in base_sets:
                if base_cols and base_cols.issubset(cols_set) and len(cols_set) > len(base_cols):
                    errors.append(
                        f"Ненужный индекс '{idx_name}' в таблице '{table}' (строка {idx_ln}):\n"
                        f" - Включает все колонки {base_name} ({', '.join(sorted(base_cols))}) и дополнительные: {', '.join([c for c in cols if c not in base_cols])}.\n"
                        "Рекомендация: удалить этот индекс или сделать его уникальным, если действительно нужен дополнительный порядок."
                    )
                    break  # находим первую базу и выходим, чтобы не дублировать

    return errors

def find_wrong_alternate_keys(sql_text):
    """
    Ищет таблицы, где PK — суррогатный (одноколоночный 'id' или '*_id'),
    но нет никакого другого UNIQUE CONSTRAINT или UNIQUE INDEX.
    """
    errors = []
    lines = sql_text.splitlines()

    # 1) Сбор всех PK
    pk_columns = {}  # {table: [col1, col2, ...]}
    current_table = None
    re_create_table = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    re_inline_pk = re.compile(r"^\s*(\w+)\s+\w+.*PRIMARY\s+KEY", re.IGNORECASE)
    re_table_pk = re.compile(r"PRIMARY\s+KEY\s*\(\s*([^)]+)\)", re.IGNORECASE)

    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m_ct = re_create_table.match(line)
        if m_ct:
            current_table = m_ct.group(1)
            continue
        if current_table and line.startswith(")"):
            current_table = None
            continue
        if current_table:
            m_inline = re_inline_pk.match(line)
            if m_inline:
                pk_columns.setdefault(current_table, []).append(m_inline.group(1).upper())
            m_table = re_table_pk.search(line)
            if m_table:
                cols = [c.strip().upper() for c in m_table.group(1).split(",")]
                pk_columns.setdefault(current_table, []).extend(cols)

    # 2) Сбор всех уникальных ключей (кроме PK)
    unique_keys = {}  # {table: [set(cols), ...]}
    # 2a) UNIQUE(...) в CREATE TABLE
    re_table_unique = re.compile(r"UNIQUE\s*\(\s*([^)]+)\)", re.IGNORECASE)
    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m_ct = re_create_table.match(line)
        if m_ct:
            current_table = m_ct.group(1)
            continue
        if current_table:
            for mu in re_table_unique.finditer(line):
                cols = {c.strip().upper() for c in mu.group(1).split(",")}
                unique_keys.setdefault(current_table, []).append(cols)
        if current_table and line.startswith(")"):
            current_table = None

    # 2b) CREATE UNIQUE INDEX
    re_unique_index = re.compile(
        r"CREATE\s+UNIQUE\s+INDEX\s+\w+\s+ON\s+(\w+)\s*\(\s*([^)]+)\)", re.IGNORECASE)
    for raw in lines:
        mu = re_unique_index.search(raw)
        if mu:
            tbl = mu.group(1)
            cols = {c.strip().upper() for c in mu.group(2).split(",")}
            unique_keys.setdefault(tbl, []).append(cols)

    # 3) Проверка для каждой таблицы
    for table, pk_cols in pk_columns.items():
        pk_cols = [c.upper() for c in pk_cols]
        # суррогатный = ровно 1 колонка, имя == ID или endswith _ID
        if len(pk_cols) == 1 and (pk_cols[0] == "ID" or pk_cols[0].endswith("_ID")):
            # проверяем, есть ли уникальные ключи, отличные от PK
            # т.е. ищем хотя бы один набор cols != {pk_cols[0]}
            alt_keys = [
                uk for uk in unique_keys.get(table, [])
                if set(pk_cols) != set(uk)
            ]
            if not alt_keys:
                errors.append(
                    f"Таблица '{table}' имеет суррогатный первичный ключ '{pk_cols[0]}' без альтернативных ключей.\n"
                    "Рекомендация: создайте альтернативный ключ (UNIQUE CONSTRAINT или UNIQUE INDEX)\n"
                    " на столбцы, обеспечивающие бизнес-уникальность строк."
                )

    return errors

def find_unnecessary_check_constraints(sql_text):
    """
    Находит неэффективные или бессмысленные ограничения CHECK:
    - IN с одним значением.
    - Простые неограничивающие выражения (age >= 0 и т.п.).
    - Тривиальные (1=1, 0=0).
    - Противоречивые условия.
    """
    errors = []
    lines = sql_text.splitlines()

    re_create_table = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    re_in_single    = re.compile(r"^\s*\w+\s+IN\s*\(\s*('[^']*'|\d+)\s*\)\s*$", re.IGNORECASE)
    re_trivial      = re.compile(r"^(1\s*=\s*1|0\s*=\s*0)$")
    re_contradict   = re.compile(r"(\w+)\s*<\s*(\d+)\s+AND\s+\1\s*>\s*(\d+)", re.IGNORECASE)
    re_gte_zero     = re.compile(r"^\s*\w+\s*>=\s*0\s*$")

    def extract_check_expr(line, start_pos):
        """
        По позиции '(' после 'CHECK' находит соответствующую ')' с учётом вложенности
        и возвращает текст выражения между ними.
        """
        depth = 0
        for i in range(start_pos, len(line)):
            if line[i] == '(':
                depth += 1
                if depth == 1:
                    expr_start = i + 1
            elif line[i] == ')':
                depth -= 1
                if depth == 0:
                    expr_end = i
                    return line[expr_start:expr_end], i
        return None, None

    current_table = None
    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()

        # Отслеживаем начало/конец CREATE TABLE
        m_tab = re_create_table.match(line)
        if m_tab:
            current_table = m_tab.group(1)
            continue
        if current_table and line.startswith(")"):
            current_table = None
            continue

        # Внутри CREATE TABLE ищем все вхождения CHECK(...)
        if current_table:
            idx = 0
            while True:
                idx = line.upper().find("CHECK", idx)
                if idx == -1:
                    break
                paren_pos = line.find("(", idx)
                if paren_pos == -1:
                    break

                expr, end_pos = extract_check_expr(line, paren_pos)
                if expr is None:
                    break
                expr = expr.strip()
                header = f"Таблица '{current_table}', строка {lineno}: CHECK ({expr})"

                # A) IN с одним значением
                if re_in_single.match(expr):
                    errors.append(
                        f"{header} — IN с одним значением.\n"
                        "Ограничение не имеет смысла: значение всегда фиксировано.\n"
                        "Рекомендация: удалите CHECK или расширьте список допустимых значений."
                    )
                # B) age >= 0 и подобные
                elif re_gte_zero.match(expr):
                    errors.append(
                        f"{header} — слишком мягкое ограничение ({expr}).\n"
                        "Оно в большинстве случаев не ограничивает круг значений.\n"
                        "Рекомендация: проверьте необходимость этого ограничения."
                    )
                # C) всегда-истинные
                elif re_trivial.match(expr):
                    errors.append(
                        f"{header} — тривиальное условие (всегда истинно).\n"
                        "Ограничение не влияет на данные. Рекомендация: удалить."
                    )
                # D) противоречивые условия (всегда ложь)
                else:
                    m_con = re_contradict.search(expr)
                    if m_con:
                        col, a, b = m_con.groups()
                        if int(a) < int(b):
                            errors.append(
                                f"{header} — логически противоречивое условие (всегда ложь).\n"
                                f"Условие: {col} < {a} AND {col} > {b}.\n"
                                "Рекомендация: пересмотрите логику ограничения."
                            )

                # продолжаем поиск после закрывающей скобки
                idx = end_pos + 1

    return errors

def find_unnecessary_foreign_keys(sql_text):
    """
    Находит «взаимно пересекающиеся» внешние ключи:
    в одной таблице есть два (или более) FK, чьи множества колонок пересекаются,
    но они ссылаются на разные родительские таблицы или на разные столбцы,
    что может приводить к конфликтам.
    """
    lines = sql_text.splitlines()

    # Шаблоны для парсинга
    re_create_table = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    re_fk = re.compile(
        r"FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+(\w+)\s*\(([^)]+)\)",
        re.IGNORECASE
    )

    # Собираем все внешние ключи: по таблице-родителю-колонкам
    # fks: { child_table: [ (child_cols_set, parent_table, parent_cols_set, lineno, raw_line) ] }
    fks = {}
    current_table = None
    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m_tab = re_create_table.match(line)
        if m_tab:
            current_table = m_tab.group(1)
            continue
        if current_table and line.startswith(")"):
            current_table = None
            continue
        if current_table:
            m_fk = re_fk.search(line)
            if m_fk:
                child_cols = {c.strip().upper() for c in m_fk.group(1).split(",")}
                parent_table = m_fk.group(2)
                parent_cols = {c.strip().upper() for c in m_fk.group(3).split(",")}
                fks.setdefault(current_table, []).append(
                    (child_cols, parent_table, parent_cols, lineno, line)
                )

    errors = []
    # Для каждой таблицы анализируем список её FKs
    for table, fk_list in fks.items():
        n = len(fk_list)
        # проверяем каждую пару FK
        for i in range(n):
            cols_i, ptab_i, pcols_i, ln_i, raw_i = fk_list[i]
            for j in range(i+1, n):
                cols_j, ptab_j, pcols_j, ln_j, raw_j = fk_list[j]
                # пересечение child_cols непусто, но либо разные parent_table, либо разные parent_cols
                if cols_i & cols_j:
                    if ptab_i != ptab_j or pcols_i != pcols_j:
                        errors.append(
                            f"Пересекающиеся связи в таблице '{table}':\n"
                            f" - FK на строке {ln_i}: {raw_i}\n"
                            f" - FK на строке {ln_j}: {raw_j}\n"
                            f"Множества колонок пересекаются: {', '.join(sorted(cols_i & cols_j))}\n"
                            "Но ссылаются на разные родительские таблицы/столбцы.\n"
                            "Рекомендация: пересмотреть дизайн внешних ключей, чтобы избежать конфликтов."
                        )
    return errors