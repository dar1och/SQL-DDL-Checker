import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from ttkthemes import ThemedStyle
from indexes_and_constraints import (
    find_null_in_primary_keys,
    find_index_anomalies,
    find_redundant_unique_indexes,
    find_tables_without_relations,
    find_missing_potential_keys,
    find_unnecessary_indexes,
    find_wrong_alternate_keys,
    find_unnecessary_check_constraints,
    find_unnecessary_foreign_keys
)  # Импортируем функции анализа
from column_modeling import (
    find_reserved_word_columns,
    find_conflicting_default_values,
    find_conflicting_column_types_with_lines
)
from realation_modeling import (
    find_undefined_relations,
    find_overlapping_relations,
    find_incorrect_recursive_hierarchy,
    find_infinite_delete_cascade
)
import os
from docx import Document

def load_sql_file(file_path):
    """Читает SQL-файл и возвращает его содержимое"""
    ext = os.path.splitext(file_path)[1].lower()
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()

def open_file():
    file_path = filedialog.askopenfilename(
        filetypes=[("SQL Files", "*.sql"), ("Text Files", "*.txt"), ("Word Documents", "*.docx")]
    )
    if file_path:
        file_path_var.set(file_path)
        file_name_var.set(file_path.split("/")[-1])
        try:
            sql_text = read_sql_file(file_path)
            # analyze_sql_text(sql_text) — если нужно вызывать анализ сразу
        except Exception as e:
            messagebox.showerror("Ошибка чтения файла", f"Не удалось прочитать файл:\n{e}")

def select_all(vars):
    for var in vars:
        var.set(1)

def analyze_sql():
    result_output.delete(1.0, tk.END)

    file_path = file_path_var.get()
    if not file_path:
        messagebox.showwarning("Ошибка", "Выберите файл для анализа!")
        return

    selected_criteria = get_selected_criteria()
    if not selected_criteria:
        messagebox.showwarning("Ошибка", "Выберите хотя бы один критерий анализа!")
        return

    sql_text = read_sql_file(file_path)
    errors = analyze_with_criteria(sql_text, selected_criteria)

    errors = list(set(errors))  # Удаление дубликатов

    if errors:
        result_output.insert(tk.END, "\n\n".join(errors))
    else:
        result_output.insert(tk.END, "Ошибок не найдено.")

def get_selected_criteria():
    selected = []
    for key, var in {**criteria_vars_1, **criteria_vars_2, **criteria_vars_3}.items():
        if var.get():
            selected.append(key)
    return selected

def read_sql_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.docx':
        try:
            doc = Document(file_path)
            return '\n'.join([para.text for para in doc.paragraphs])
        except Exception as e:
            raise RuntimeError(f"Ошибка чтения .docx файла: {e}")
    else:
        # Пробуем UTF-8 сначала, затем CP1251
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return file.read()
        except UnicodeDecodeError:
            try:
                with open(file_path, 'r', encoding='cp1251') as file:
                    return file.read()
            except Exception as e:
                raise RuntimeError(f"Ошибка чтения файла: {e}")

def analyze_with_criteria(sql_text, selected_criteria):
    criteria_functions = {
        "null_primary_key": find_null_in_primary_keys,
        "reserved_words": find_reserved_word_columns,
        "default_value_conflict": find_conflicting_default_values,
        "conflicting_types": find_conflicting_column_types_with_lines,
        "undefined_relations": find_undefined_relations,
        "index_anomalies": find_index_anomalies,
        "redundant_unique_index": find_redundant_unique_indexes,
        "tables_without_relations": find_tables_without_relations,
        "missing_potential_keys": find_missing_potential_keys,
        "unnecessary_indexes": find_unnecessary_indexes,
        "wrong_alternate_key": find_wrong_alternate_keys,
        "unnecessary_check_constraints": find_unnecessary_check_constraints,
        "unnecessary_foreign_keys": find_unnecessary_foreign_keys,
        "overlapping_relations": find_overlapping_relations,
        "incorrect_recursive_relation": find_incorrect_recursive_hierarchy,
        "infinite_delete_cascade": find_infinite_delete_cascade
    }

    errors = []
    for criterion in selected_criteria:
        func = criteria_functions.get(criterion)
        if func:
            errors.extend(func(sql_text))
    return errors

def change_theme(event):
    selected_theme = theme_var.get()
    style.set_theme(selected_theme)
    result_output.configure(bg=style.lookup('TLabel', 'background'), fg=style.lookup('TLabel', 'foreground'))

# --- Создаем GUI ---
root = tk.Tk()
root.title("SQL DDL Checker")

# Устанавливаем темную тему по умолчанию
style = ThemedStyle(root)
style.set_theme("equilux")

# Основной фрейм для левой части
left_frame = ttk.Frame(root)
left_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")

# Поле для отображения пути к файлу
file_path_var = tk.StringVar()
file_name_var = tk.StringVar()

frame_file = ttk.Frame(left_frame)
frame_file.pack(pady=10, fill="x", padx=10)

ttk.Label(frame_file, text="Выбранный файл:").pack(side="left")
ttk.Entry(frame_file, textvariable=file_name_var, state="readonly", width=40).pack(side="left", padx=5)
ttk.Button(frame_file, text="Выбрать файл", command=open_file).pack(side="right")

# Выбор темы
theme_var = tk.StringVar()
theme_combobox = ttk.Combobox(left_frame, textvariable=theme_var, state="readonly")
theme_combobox['values'] = style.get_themes()
theme_combobox.current(style.get_themes().index("equilux"))
theme_combobox.pack(pady=10)
theme_combobox.bind("<<ComboboxSelected>>", change_theme)

# --- Категория 1: Ошибки моделирования колонок ---
frame_criteria1 = ttk.LabelFrame(left_frame, text="Ошибки моделирования колонок (Категория 1)")
frame_criteria1.pack(pady=5, padx=10, fill="x")

criteria_vars_1 = {
    "conflicting_types": tk.IntVar(),
    "default_value_conflict": tk.IntVar(),
    "reserved_words": tk.IntVar(),
}

criteria_labels_1 = {
    "conflicting_types": "Противоречивые типы колонок",
    "default_value_conflict": "Противоречивые значения по умолчанию",
    "reserved_words": "Резервированные слова",
}

for key, text in criteria_labels_1.items():
    ttk.Checkbutton(frame_criteria1, text=text, variable=criteria_vars_1[key]).pack(anchor="w")

ttk.Button(frame_criteria1, text= "Выбрать все", command=lambda: select_all(criteria_vars_1.values())).pack(pady=2)

# --- Категория 2: Ошибки индексов и ограничений ---
frame_criteria2 = ttk.LabelFrame(left_frame, text="Ошибки индексов и ограничений (Категория 2)")
frame_criteria2.pack(pady=5, padx=10, fill="x")

criteria_vars_2 = {
    "null_primary_key": tk.IntVar(),
    "index_anomalies": tk.IntVar(),
    "wrong_alternate_key": tk.IntVar(),
    "missing_potential_keys": tk.IntVar(),
    "unnecessary_check_constraints": tk.IntVar(),
    "unnecessary_indexes": tk.IntVar(),
    "unnecessary_foreign_keys": tk.IntVar(),
    "redundant_unique_index": tk.IntVar(),
    "tables_without_relations": tk.IntVar(),
}

criteria_labels_2 = {
    "null_primary_key": "NULL-значение для первичного ключа",
    "index_anomalies": "Аномалии в определении индексов",
    "wrong_alternate_key": "Некорректный альтернативный ключ",
    "missing_potential_keys": "Отсутствие потенциальных ключей",
    "unnecessary_check_constraints": "Ненужные ограничения CHECK",
    "unnecessary_indexes": "Ненужные индексы",
    "unnecessary_foreign_keys": "Ненужный внешний ключ",
    "redundant_unique_index": "Уникальный индекс, эквивалентный первичному ключу",
    "tables_without_relations": "Таблицы без связей",
}

for key, text in criteria_labels_2.items():
    ttk.Checkbutton(frame_criteria2, text=text, variable=criteria_vars_2[key]).pack(anchor="w")

ttk.Button(frame_criteria2, text="Выбрать все", command=lambda: select_all(criteria_vars_2.values())).pack(pady=2)

# --- Категория 3: Ошибки моделирования связей ---
frame_criteria3 = ttk.LabelFrame(left_frame, text="Ошибки моделирования связей (Категория 3)")
frame_criteria3.pack(pady=5, padx=10, fill="x")

criteria_vars_3 = {
    "incorrect_recursive_relation": tk.IntVar(),
    "undefined_relations": tk.IntVar(),
    "infinite_delete_cascade": tk.IntVar(),
    "overlapping_relations": tk.IntVar(),
}

criteria_labels_3 = {
    "incorrect_recursive_relation": "Некорректная иерархическая рекурсивная связь",
    "undefined_relations": "Неопределенные связи между таблицами",
    "infinite_delete_cascade": "Бесконечные циклы при Delete Cascade",
    "overlapping_relations": "Взаимно пересекающиеся связи",
}

for key, text in criteria_labels_3.items():
    ttk.Checkbutton(frame_criteria3, text=text, variable=criteria_vars_3[key]).pack(anchor="w")

ttk.Button(frame_criteria3, text="Выбрать все", command=lambda: select_all(criteria_vars_3.values())).pack(pady=2)

# Кнопка анализа
btn_analyze = ttk.Button(left_frame, text="Проверить", command=analyze_sql)
btn_analyze.pack(pady=10)

# Поле для вывода ошибок справа
frame_output = ttk.Frame(root)
frame_output.grid(row=0, column=1, padx=10, pady=10, sticky="nsew")

scrollbar = ttk.Scrollbar(frame_output, orient="vertical")
scrollbar.pack(side="right", fill="y")

result_output = tk.Text(frame_output, wrap="word", height=20, width=60, yscrollcommand=scrollbar.set)
result_output.pack(fill="both", expand=True)

scrollbar.config(command=result_output.yview)

# Настройка расширения виджетов при изменении размера окна
root.grid_rowconfigure(0, weight=1)
root.grid_columnconfigure(1, weight=1)

# Запуск
root.mainloop()