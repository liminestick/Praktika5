import json
from pymongo import MongoClient

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["job_database"]
collection = db["jobs"]

# Чтение данных из файла JSON и добавление в коллекцию
json_file = "2/task_2_item.json"
with open(json_file, "r", encoding="utf-8") as file:
    data = json.load(file)
    for entry in data:
        entry["salary"] = int(entry["salary"])
        entry["age"] = int(entry["age"])
        entry["year"] = int(entry["year"])
        collection.insert_one(entry)

# Запросы
# 1. Минимальная, средняя, максимальная salary
pipeline_salary = [
    {"$group": {
        "_id": None,
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }}
]
salary_stats = list(collection.aggregate(pipeline_salary))[0]
print("\n1. Статистика по зарплате:")
print(salary_stats)

# 2. Количество данных по профессиям
pipeline_professions = [
    {"$group": {
        "_id": "$job",
        "count": {"$sum": 1}
    }}
]
professions_stats = list(collection.aggregate(pipeline_professions))
print("\n2. Количество данных по профессиям:")
print(professions_stats)

# 3. Минимальная, средняя, максимальная salary по городу
pipeline_salary_city = [
    {"$group": {
        "_id": "$city",
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }}
]
salary_city_stats = list(collection.aggregate(pipeline_salary_city))
print("\n3. Статистика по зарплате по городам:")
print(salary_city_stats)

# 4. Минимальная, средняя, максимальная salary по профессии
pipeline_salary_job = [
    {"$group": {
        "_id": "$job",
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }}
]
salary_job_stats = list(collection.aggregate(pipeline_salary_job))
print("\n4. Статистика по зарплате по профессиям:")
print(salary_job_stats)

# 5. Минимальный, средний, максимальный возраст по городу
pipeline_age_city = [
    {"$group": {
        "_id": "$city",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }}
]
age_city_stats = list(collection.aggregate(pipeline_age_city))
print("\n5. Статистика по возрасту по городам:")
print(age_city_stats)

# 6. Минимальный, средний, максимальный возраст по профессии
pipeline_age_job = [
    {"$group": {
        "_id": "$job",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }}
]
age_job_stats = list(collection.aggregate(pipeline_age_job))
print("\n6. Статистика по возрасту по профессиям:")
print(age_job_stats)

# 7. Максимальная зарплата при минимальном возрасте
pipeline_max_salary_min_age = [
    {"$sort": {"age": 1, "salary": -1}},
    {"$limit": 1}
]
max_salary_min_age = list(collection.aggregate(pipeline_max_salary_min_age))[0]
print("\n7. Максимальная зарплата при минимальном возрасте:")
print(max_salary_min_age)

# 8. Минимальная зарплата при максимальном возрасте
pipeline_min_salary_max_age = [
    {"$sort": {"age": -1, "salary": 1}},
    {"$limit": 1}
]
min_salary_max_age = list(collection.aggregate(pipeline_min_salary_max_age))[0]
print("\n8. Минимальная зарплата при максимальном возрасте:")
print(min_salary_max_age)

# 9. Статистика возраста по городам при salary > 50000, сортировка по убыванию avg_age
pipeline_age_salary = [
    {"$match": {"salary": {"$gt": 50000}}},
    {"$group": {
        "_id": "$city",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }},
    {"$sort": {"avg_age": -1}}
]
age_salary_stats = list(collection.aggregate(pipeline_age_salary))
print("\n9. Статистика возраста по городам с salary > 50000, сортировка по avg_age:")
print(age_salary_stats)

# 10. Статистика по заданным диапазонам
pipeline_ranges = [
    {"$match": {"age": {"$gt": 18, "$lt": 25}}},
    {"$group": {
        "_id": {"city": "$city", "job": "$job"},
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }}
]
ranges_stats = list(collection.aggregate(pipeline_ranges))
print("\n10. Статистика по заданным диапазонам:")
print(ranges_stats)

# 11. Произвольный запрос с $match, $group, $sort
pipeline_custom = [
    {"$match": {"age": {"$gte": 30}}},
    {"$group": {
        "_id": "$job",
        "total_salary": {"$sum": "$salary"}
    }},
    {"$sort": {"total_salary": -1}}
]
custom_stats = list(collection.aggregate(pipeline_custom))
print("\n11. Произвольный запрос (суммарная зарплата по профессиям для возраста >= 30):")
print(custom_stats)

# Завершение работы
client.close()
