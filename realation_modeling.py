import re
from collections import defaultdict

def find_undefined_relations(sql_text):
    """
    Группирует колонки по (имя, тип) и находит те, что встречаются в ≥2 таблицах,
    но не являются PK во всех таблицах и не связаны через FOREIGN KEY.
    Игнорирует строки ограничения (CONSTRAINT, PRIMARY KEY, FOREIGN KEY и т.д.).
    """
    lines = sql_text.splitlines()

    # 1) Собираем определения столбцов
    re_table  = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    re_column = re.compile(r"^\s*(\w+)\s+([\w()]+)", re.IGNORECASE)

    groups = {}  # { (col_name, typ): [(table, line)] }
    current_table = None

    constraint_keywords = ("CONSTRAINT", "PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK", "REFERENCES")

    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m_t = re_table.match(line)
        if m_t:
            current_table = m_t.group(1)
            continue
        if current_table and line.startswith(")"):
            current_table = None
            continue
        if current_table:
            # пропускаем строки ограничений
            if any(kw in line.upper() for kw in constraint_keywords):
                continue
            m_c = re_column.match(line)
            if m_c:
                name, typ = m_c.groups()
                key = (name.upper(), typ.upper())
                groups.setdefault(key, []).append((current_table, lineno))

    # 2) Собираем PK-колонки
    pk_cols = set()
    # inline PK
    re_inline_pk = re.compile(r"^\s*(\w+)\s+[\w()]+.*PRIMARY\s+KEY", re.IGNORECASE)
    current_table = None
    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m_t = re_table.match(line)
        if m_t:
            current_table = m_t.group(1)
            continue
        if current_table and line.startswith(")"):
            current_table = None
            continue
        if current_table:
            if any(kw in line.upper() for kw in constraint_keywords):
                continue
            m_pk = re_inline_pk.match(line)
            if m_pk:
                pk_cols.add((current_table, m_pk.group(1).upper()))

    # table-level PK
    re_table_pk = re.compile(r"PRIMARY\s+KEY\s*\(\s*([^)]+)\)", re.IGNORECASE)
    current_table = None
    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m_t = re_table.match(line)
        if m_t:
            current_table = m_t.group(1)
            continue
        if current_table:
            m_pk = re_table_pk.search(line)
            if m_pk:
                for col in m_pk.group(1).split(","):
                    pk_cols.add((current_table, col.strip().upper()))

    # 3) Собираем FK-колонки
    fkc = set()  # child cols
    fkp = set()  # parent cols

    re_inline_fk = re.compile(r"REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)", re.IGNORECASE)
    for lineno, raw in enumerate(lines, start=1):
        for m in re_inline_fk.finditer(raw):
            pt, pc = m.groups()
            # найдём текущую таблицу и child-col
            line_strip = raw.strip()
            mc = re_column.match(line_strip)
            if not mc: continue
            cc = mc.group(1).upper()
            # ищем table вверх
            j = lineno-1
            child_table = None
            while j>0:
                mt = re_table.match(lines[j-1].strip())
                if mt:
                    child_table = mt.group(1); break
                j -= 1
            if child_table:
                fkc.add((child_table, cc))
                fkp.add((pt, pc.upper()))

    # table-level FK
    re_table_fk = re.compile(
        r"FOREIGN\s+KEY\s*\(\s*(\w+)\s*\).*?REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)",
        re.IGNORECASE)
    current_table = None
    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        m_t = re_table.match(line)
        if m_t:
            current_table = m_t.group(1)
            continue
        if current_table:
            mf = re_table_fk.search(line)
            if mf:
                cc, pt, pc = mf.groups()
                fkc.add((current_table, cc.upper()))
                fkp.add((pt, pc.upper()))

    # 4) Формируем окончательные группы ошибок
    errors = []
    for (col_name, typ), occ in groups.items():
        tables = {t for t, _ in occ}
        if len(tables) < 2:
            continue

        # если **все** вхождения — PK, пропустить
        if all((t, col_name) in pk_cols for t in tables):
            continue

        # если есть FK между какими-то парами — пропустить
        related = False
        for t1, _ in occ:
            for t2, _ in occ:
                if (t1, col_name) in fkc and (t2, col_name) in fkp:
                    related = True
                if (t2, col_name) in fkc and (t1, col_name) in fkp:
                    related = True
        if related:
            continue

        # иначе — выводим одну группу
        info_lines = []
        for t, ln in occ:
            flag = "(PK)" if (t, col_name) in pk_cols else ""
            info_lines.append(f" - таблица '{t}', строка {ln} {flag}".rstrip())
        table_list = ", ".join(sorted(tables))
        msg = (
            f"Неопределённая связь для колонки '{col_name}' (тип {typ}):\n"
            + "\n".join(info_lines) + "\n"
            + f"Таблицы: {table_list}\n"
            "Рекомендация: проверьте бизнес-правила и при необходимости "
            "установите FOREIGN KEY между таблицами."
        )
        errors.append(msg)

    return errors

def find_overlapping_relations(sql_text):
    """
    Находит взаимно пересекающиеся связи между двумя таблицами по разным колонкам:
    Таблица A ссылается на B по колонке X, а B ссылается на A по колонке Y.
    Такая схема приводит к невозможности вставки данных.
    """
    errors = []
    lines = sql_text.splitlines()
    re_create = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    re_fk = re.compile(
        r"FOREIGN\s+KEY\s*\(\s*(\w+)\s*\)\s+REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)",
        re.IGNORECASE
    )

    current_table = None
    table_stack = []
    relations = []  # [(from_table, from_col, to_table, to_col)]

    for line in lines:
        m_create = re_create.match(line)
        if m_create:
            current_table = m_create.group(1)
            table_stack.append(current_table)
            continue
        if current_table and line.strip().startswith(")"):
            table_stack.pop()
            current_table = table_stack[-1] if table_stack else None
            continue
        if current_table:
            m_fk = re_fk.search(line)
            if m_fk:
                from_col, to_table, to_col = m_fk.groups()
                relations.append((current_table, from_col, to_table, to_col))

    # Сопоставим связи между парами таблиц
    pair_map = defaultdict(list)
    for rel in relations:
        key = tuple(sorted([rel[0], rel[2]]))  # (A, B)
        pair_map[key].append(rel)

    # Найдём пары с двумя взаимными связями
    for pair, rels in pair_map.items():
        if len(rels) == 2:
            a, b = pair
            rel_1 = rels[0]
            rel_2 = rels[1]
            if rel_1[0] != rel_2[0]:  # связи в обе стороны
                errors.append(
                    f"Взаимно пересекающиеся связи между таблицами '{a}' и '{b}':\n"
                    f" - {rel_1[0]}.{rel_1[1]} → {rel_1[2]}.{rel_1[3]}\n"
                    f" - {rel_2[0]}.{rel_2[1]} → {rel_2[2]}.{rel_2[3]}\n"
                    f"Ошибка: такая схема делает невозможным вставку данных без нарушения ссылочной целостности.\n"
                    f"Решение: избегайте двусторонних зависимостей — используйте однонаправленные связи или отложенные ограничения."
                )

    return errors

def find_incorrect_recursive_hierarchy(sql_text):
    """
    Ищет некорректные иерархические связи, когда таблица содержит внешний ключ на саму себя,
    и при этом колонка, на которую ссылается внешний ключ, объявлена как NOT NULL.
    """

    errors = []
    lines = sql_text.splitlines()

    re_create = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)
    re_column_def = re.compile(r"^\s*(\w+)\s+\w+.*", re.IGNORECASE)
    re_fk = re.compile(r"FOREIGN\s+KEY\s*\((\w+)\)\s+REFERENCES\s+(\w+)", re.IGNORECASE)
    re_not_null_column = re.compile(r"(\w+)\s+\w+.*NOT\s+NULL", re.IGNORECASE)

    current_table = None
    table_columns = defaultdict(dict)  # table -> column -> {'not_null': True/False}
    foreign_keys = defaultdict(list)   # table -> list of (column, ref_table)

    for line in lines:
        create_match = re_create.search(line)
        if create_match:
            current_table = create_match.group(1)
            continue

        if current_table:
            # Определение NOT NULL колонок
            nn_match = re_not_null_column.search(line)
            if nn_match:
                col_name = nn_match.group(1)
                table_columns[current_table][col_name] = {'not_null': True}
            else:
                # Колонка без NOT NULL
                col_match = re_column_def.search(line)
                if col_match:
                    col_name = col_match.group(1)
                    if col_name not in table_columns[current_table]:
                        table_columns[current_table][col_name] = {'not_null': False}

            # Определение внешних ключей
            fk_match = re_fk.search(line)
            if fk_match:
                fk_col, ref_table = fk_match.groups()
                foreign_keys[current_table].append((fk_col, ref_table))

    # Проверка: есть ли самоссылка с NOT NULL
    for table, fks in foreign_keys.items():
        for col, ref_table in fks:
            if ref_table == table:  # Самоссылка
                col_info = table_columns[table].get(col, {})
                if col_info.get('not_null'):
                    errors.append(
                        f"Ошибка: некорректная иерархическая самоссылка в таблице '{table}'.\n"
                        f" - Колонка '{col}' ссылается на ту же таблицу и имеет ограничение NOT NULL.\n"
                        f" - Это делает невозможным наличие вершины иерархии.\n"
                        f"Решение: разрешите значение NULL для поля '{col}'."
                    )

    return errors

def find_infinite_delete_cascade(sql_text):
    """
    Ищет потенциальные бесконечные циклы удаления ON DELETE CASCADE в DDL-скрипте.
    Разбиваем на statements по ';', в каждом сразу ищем TABLE и все CASCADE-FK.
    Возвращаем список ошибок (каждая — с полным циклом таблиц).
    """
    errors = []
    # Регулярки
    stmt_re = re.compile(r'(?:CREATE|ALTER)\s+TABLE\s+(\w+)', re.IGNORECASE)
    fk_re = re.compile(
        r'FOREIGN\s+KEY\s*\(\w+\)\s+REFERENCES\s+(\w+)\s*\(\w+\)\s*ON\s+DELETE\s+CASCADE',
        re.IGNORECASE
    )

    graph = defaultdict(list)
    # Разделяем на statements (к удалённым точкам с запятой)
    statements = re.split(r';\s*', sql_text)
    for stmt in statements:
        m = stmt_re.search(stmt)
        if not m:
            continue
        tbl = m.group(1)
        # найдём все target-таблицы в этом statement
        for tgt in fk_re.findall(stmt):
            graph[tbl].append(tgt)

    # Собираем все имена таблиц
    all_tables = set(graph.keys())
    for targets in graph.values():
        all_tables.update(targets)

    # Функция DFS, возвращающая первый найденный цикл как список таблиц
    def dfs(node, path, visited):
        path.append(node)
        visited.add(node)
        for neigh in graph.get(node, []):
            if neigh in path:
                # цикл: вернём участок от первой встречи до конца
                idx = path.index(neigh)
                return path[idx:]
            if neigh not in visited:
                res = dfs(neigh, path.copy(), visited.copy())
                if res:
                    return res
        return None

    # Пробуем из каждой таблицы
    for tbl in all_tables:
        cycle = dfs(tbl, [], set())
        if cycle:
            cycle_str = " -> ".join(cycle + [cycle[0]])
            errors.append(
                f"Ошибка: обнаружен цикл каскадных удалений между таблицами:\n"
                f"  {cycle_str}\n"
                "- Это может привести к бесконечному циклу при ON DELETE CASCADE.\n"
                "Решение: удалите хотя бы одну ON DELETE CASCADE зависимость из цикла."
            )
            break
    return errors