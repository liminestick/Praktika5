import msgpack
from pymongo import MongoClient

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["job_database"]
collection = db["jobs"]

# Чтение данных из msgpack файла
msgpack_file = "3/task_3_item.msgpack"
with open(msgpack_file, "rb") as file:
    data = msgpack.unpackb(file.read(), strict_map_key=False)

# Добавление данных из msgpack файла в коллекцию MongoDB
collection.insert_many(data)
print("Данные из msgpack добавлены в коллекцию.")

# Задание 1: Удалить документы по предикату salary < 25000 или salary > 175000
delete_predicate = {"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]}
delete_result = collection.delete_many(delete_predicate)
print(f"Удалено {delete_result.deleted_count} документов по предикату.")

# Задание 2: Увеличить возраст (age) всех документов на 1
update_age_result = collection.update_many({}, {"$inc": {"age": 1}})
print(f"Обновлено {update_age_result.modified_count} документов: возраст увеличен на 1.")

# Задание 3: Поднять зарплату на 5% для произвольно выбранных профессий
professions = ["Психолог", "Косметолог"]  # Произвольный набор профессий
update_salary_professions = collection.update_many(
    {"job": {"$in": professions}}, {"$mul": {"salary": 1.05}}
)
print(f"Обновлено {update_salary_professions.modified_count} документов: зарплата увеличена на 5% для профессий {professions}.")

# Задание 4: Поднять зарплату на 7% для произвольно выбранных городов
cities = ["Москва", "Будапешт"]  # Произвольный набор городов
update_salary_cities = collection.update_many(
    {"city": {"$in": cities}}, {"$mul": {"salary": 1.07}}
)
print(f"Обновлено {update_salary_cities.modified_count} документов: зарплата увеличена на 7% для городов {cities}.")

# Задание 5: Поднять зарплату на 10% для выборки по сложному предикату
complex_predicate = {
    "$and": [
        {"city": "Москва"},  # Произвольный город
        {"job": {"$in": ["Повар", "Психолог"]}},  # Произвольные профессии
        {"age": {"$gte": 30, "$lte": 60}}  # Произвольный диапазон возраста
    ]
}
update_salary_complex = collection.update_many(complex_predicate, {"$mul": {"salary": 1.10}})
print(f"Обновлено {update_salary_complex.modified_count} документов: зарплата увеличена на 10% для сложного предиката.")

# Задание 6: Удалить записи по произвольному предикату
random_delete_predicate = {"city": "Сьюдад-Реаль"}  # Произвольный предикат
delete_random = collection.delete_many(random_delete_predicate)
print(f"Удалено {delete_random.deleted_count} документов по произвольному предикату ({random_delete_predicate}).")

# Завершение работы
client.close()
print("Все операции завершены.")
