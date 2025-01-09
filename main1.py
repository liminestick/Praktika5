import csv
from pymongo import MongoClient

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["job_database"]
collection = db["jobs"]

# Чтение данных из CSV и загрузка в MongoDB
csv_file = "1/task_1_item.csv"
with open(csv_file, "r", encoding="utf-8") as file:
    reader = csv.DictReader(file, delimiter=";")
    for row in reader:
        row["salary"] = int(row["salary"])
        row["age"] = int(row["age"])
        row["year"] = int(row["year"])
        collection.insert_one(row)

# Запросы
# 1. Первые 10 записей, отсортированные по убыванию по полю salary
query1 = collection.find().sort("salary", -1).limit(10)
print("1. Первые 10 записей, отсортированные по убыванию по salary:")
for doc in query1:
    print(doc)

# 2. Первые 15 записей, age < 30, отсортированные по убыванию по полю salary
query2 = collection.find({"age": {"$lt": 30}}).sort("salary", -1).limit(15)
print("\n2. Первые 15 записей с age < 30, отсортированные по убыванию по salary:")
for doc in query2:
    print(doc)

# 3. Первые 10 записей по сложному предикату
city = "Мадрид"  # Произвольный город
professions = ["Учитель", "Программист", "IT-специалист"]  # Произвольные профессии
query3 = collection.find(
    {"$and": [{"city": city}, {"job": {"$in": professions}}]}
).sort("age", 1).limit(10)
print("\n3. Первые 10 записей по сложному предикату (город и профессии):")
for doc in query3:
    print(doc)

# 4. Количество записей по сложному предикату
age_range = {"$gte": 20, "$lte": 40}  # Произвольный диапазон возраста
query4 = collection.find({
    "$and": [
        {"age": age_range},
        {"year": {"$gte": 2019, "$lte": 2022}},
        {"$or": [
            {"salary": {"$gt": 50000, "$lte": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}}
        ]}
    ]
})
count = collection.count_documents({
    "$and": [
        {"age": age_range},
        {"year": {"$gte": 2019, "$lte": 2022}},
        {"$or": [
            {"salary": {"$gt": 50000, "$lte": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}}
        ]}
    ]
})
print("\n4. Количество записей по фильтру:")
print(count)

# Завершение работы
client.close()
