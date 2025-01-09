import csv
import json
from pymongo import MongoClient

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["users_database"]
collection = db["users"]

# Очистка коллекции перед загрузкой новых данных
collection.delete_many({})

# Загрузка данных из CSV
csv_file = "4/users.csv"
with open(csv_file, "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        collection.insert_one(row)

# Загрузка данных из XML
xml_file = "4/users.xml"
with open(xml_file, "r", encoding="utf-8") as file:
    content = file.read()

# Разбор XML вручную
users = []
while "<Sheet1>" in content:
    start = content.index("<Sheet1>") + len("<Sheet1>")
    end = content.index("</Sheet1>")
    user_data = content[start:end]
    user = {}

    for field in ["name", "phoneNumber", "email", "address", "userAgent", "hexcolor"]:
        if f"<{field}>" in user_data:
            field_start = user_data.index(f"<{field}>") + len(f"<{field}>")
            field_end = user_data.index(f"</{field}>")
            user[field] = user_data[field_start:field_end]

    users.append(user)
    content = content[end + len("</Sheet1>"):]

# Вставка данных из XML в MongoDB
collection.insert_many(users)

# Запросы
# 1. Простая выборка: найти всех пользователей с определённым доменом почты
domain = "gmail.com"
users_with_domain = list(collection.find({"email": {"$regex": f"@{domain}"}}, {"_id": 0}))
print(f"Пользователи с доменом {domain}:", users_with_domain)

# 2. Агрегация: подсчёт количества пользователей с каждым hexcolor
hexcolor_counts = list(collection.aggregate([
    {"$group": {"_id": "$hexcolor", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]))
print("Количество пользователей по hexcolor:", hexcolor_counts)

# 3. Обновление: добавить поле с длиной имени
collection.update_many({}, {"$set": {"name_length": {"$strLenCP": "$name"}}})
print("Добавлено поле name_length для всех документов.")

# 4. Удаление: удалить пользователей с определённым hexcolor
delete_color = "#342768"
collection.delete_many({"hexcolor": delete_color})
print(f"Удалены пользователи с hexcolor {delete_color}.")

# 5. Сложный запрос: найти всех пользователей с длинным адресом (> 25 символов)
long_address_users = list(collection.find({"address": {"$regex": ".{25,}"}}, {"_id": 0}))
print("Пользователи с длинным адресом:", long_address_users)

# Завершение работы
client.close()
