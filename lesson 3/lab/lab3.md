#  Лабораторная работа: «Анатомия транзакции в WiredTiger»

##  Цель работы

На практике увидеть, как WiredTiger обеспечивает **ACID-свойства** через MVCC, оптимистичный контроль конкурентности, History Store и журнал. Студент должен *почувствовать* транзакцию изнутри: увидеть конфликты, откаты, снапшоты и разницу между уровнями долговечности.


**Стек:** MongoDB (движок WiredTiger), `mongosh`, Python (`pymongo` + `threading`)
**Уровень:** средний (нужны базовые знания MongoDB и Python)

---

## Теоретическая база 

Перед выполнением освежите в памяти:

| Компонент | Роль в транзакции |
|---|---|
| **Snapshot** | Фиксирует видимое состояние БД на момент старта транзакции (Snapshot Isolation) |
| **History Store** | Хранит старые версии записей, чтобы другие транзакции могли читать старые снапшоты |
| **OCC** | Транзакции не блокируют друг друга; при конфликте одна получает `WriteConflict` |
| **Journal (WAL)** | Обеспечивает durability на уровне коммита (`writeConcern`) |
| **Checkpoint** | Периодический сброс на диск (durability на уровне снапшота) |

---

## Подготовка окружения

### Вариант A: локальный Docker (рекомендуется)

```bash
# 1. Запуск single-node replica set (транзакции требуют replica set!)
docker run -d --name wt-lab -p 27017:27017 mongo:7 --replSet rs0 --bind_ip_all

# 2. Инициализация replica set
docker exec -it wt-lab mongosh --eval "rs.initiate()"

# 3. Проверка
docker exec -it wt-lab mongosh --eval "rs.status().ok"
```

>  **Почему replica set?** Мультидокументные транзакции в MongoDB работают только на replica set или шардированном кластере. Это прямое следствие того, что WiredTiger использует журнал репликации для координации коммитов.



### Установка зависимостей Python

```bash
pip install pymongo
```

---

## Эксперимент 1: «Снапшот — это машина времени»

**Гипотеза:** Транзакция видит данные на момент своего старта, даже если другие транзакции изменили их позже.

### Скрипт `exp1_snapshot.py`

```python
from pymongo import MongoClient
from pymongo.errors import OperationFailure
import time

client = MongoClient("mongodb://localhost:27017/?replicaSet=rs0")
db = client.lab
coll = db.accounts
coll.delete_many({})

# Инициализация: два счёта по 1000
coll.insert_many([
    {"_id": "A", "balance": 1000},
    {"_id": "B", "balance": 1000},
])

# --- Транзакция T1: начинает и "замирает" ---
s1 = client.start_session()
s1.start_transaction()
before = coll.find_one({"_id": "A"}, session=s1)
print(f"[T1] Видит A = {before['balance']}  (ожидаем 1000)")

# --- Пока T1 "спит", другая сессия меняет A ---
s2 = client.start_session()
coll.update_one({"_id": "A"}, {"$inc": {"balance": 500}}, session=s2)
print(f"[T2] Изменила A на +500 (вне транзакции, авто-коммит)")

# --- T1 снова читает A: должна видеть СТАРОЕ значение! ---
after = coll.find_one({"_id": "A"}, session=s1)
print(f"[T1] Повторно видит A = {after['balance']}  (ожидаем всё ещё 1000!)")

# --- А теперь T1 делает запись в A ---
try:
    coll.update_one({"_id": "A"}, {"$inc": {"balance": -200}}, session=s1)
    s1.commit_transaction()
    print("[T1] Коммит прошёл — но что стало с балансом?")
except OperationFailure as e:
    print(f"[T1] Конфликт при коммите: {e}")
    s1.abort_transaction()
finally:
    final = coll.find_one({"_id": "A"})
    print(f"[ФИНАЛ] A = {final['balance']}")
```

### Вопросы для размышления

1. На шаге «повторно видит A» — почему T1 всё ещё видит `1000`? Где физически хранится версия `1000`, если в B-Tree уже лежит `1500`? **(подсказка: History Store)**
2. Что произошло при попытке записи в T1? Получили ли вы `WriteConflict`? Если нет — почему? (подумайте, обновляли ли вы *ту же самую версию* записи).
3. **Модифицируйте скрипт:** заставьте T1 и T2 обновить *один и тот же документ* так, чтобы гарантированно получить конфликт. Опишите, что меняется в поведении.

---

## Эксперимент 2: «Гонка транзакций — кто кого?»

**Гипотеза:** При оптимистичном контроле одна из конфликтующих транзакций получит ошибку и должна быть повторена. Никакого «тихого» lost update не будет.

### Скрипт `exp2_race.py`

```python
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from concurrent.futures import ThreadPoolExecutor
import threading

client = MongoClient("mongodb://localhost:27017/?replicaSet=rs0")
db = client.lab
coll = db.counter
coll.delete_many({})
coll.insert_one({"_id": "x", "value": 0})

barrier = threading.Barrier(2)  # обе транзакции ждут друг друга

def worker(name, increment):
    session = client.start_session()
    try:
        session.start_transaction()
        doc = coll.find_one({"_id": "x"}, session=session)
        barrier.wait()  # обе прочитали одно и то же значение
        new_val = doc["value"] + increment
        coll.update_one({"_id": "x", "value": doc["value"]},
                        {"$set": {"value": new_val}}, session=session)
        session.commit_transaction()
        print(f"[{name}]  Коммит, value = {new_val}")
    except OperationFailure as e:
        print(f"[{name}]  Конфликт: {e}")
        session.abort_transaction()

with ThreadPoolExecutor(max_workers=2) as ex:
    f1 = ex.submit(worker, "T1", 10)
    f2 = ex.submit(worker, "T2", 20)
    f1.result(); f2.result()

print(f"[ФИНАЛ] counter = {coll.find_one({'_id': 'x'})['value']}")
print("Ожидание: ровно одно обновление применилось (10 ИЛИ 20), второе — конфликт")
```

### Задания

1. Запустите скрипт **10 раз**. Сколько раз выигрывала T1, сколько T2? Есть ли случаи «потерянного обновления» (значение 30 — это баг)?
2. **Добавьте retry-логику**: оберните тело транзакции в цикл `while True` с повторной попыткой при `WriteConflict`. Убедитесь, что итог всегда `30`. Это — паттерн, который MongoDB рекомендует в документации.
3. **Модификация:** замените фильтр в `update_one` на просто `{"_id": "x"}` (без проверки `value`). Что изменилось? Почему конфликт может не возникнуть? Как это связано с «check-and-set» vs «blind write»?

---

## Эксперимент 3: «Атомарность перевода денег»

**Гипотеза:** Если в середине транзакции «уронить» её (abort), ни один документ не изменится — даже если первый уже был обновлён в памяти.

### Скрипт `exp3_atomicity.py`

```python
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/?replicaSet=rs0")
db = client.lab
coll = db.accounts
coll.delete_many({})
coll.insert_many([
    {"_id": "A", "balance": 1000},
    {"_id": "B", "balance": 1000},
])

session = client.start_session()
session.start_transaction()
try:
    # Списываем с A
    coll.update_one({"_id": "A", "balance": {"$gte": 500}},
                    {"$inc": {"balance": -500}}, session=session)
    print("[T] A списан на 500")

    # Проверяем "внутри транзакции" — что видит сама транзакция?
    print("[T] внутри: A =", coll.find_one({"_id": "A"}, session=session)["balance"])

    # Искусственная ошибка — эмулируем сбой бизнес-логики
    raise RuntimeError("Бизнес-ошибка: перевод отклонён банком-корреспондентом!")

except RuntimeError as e:
    print(f"[T] Откат: {e}")
    session.abort_transaction()

# Проверяем снаружи
print("[ВНЕ] A =", coll.find_one({"_id": "A"})["balance"], " (ожидаем 1000)")
print("[ВНЕ] B =", coll.find_one({"_id": "B"})["balance"], " (ожидаем 1000)")
```

### Задания

1. Что вывела строка «внутри транзакции» — `500` или `1000`? Объясните: почему транзакция видит *свои* изменения, но другие её не видят? **(подсказка: снапшот + uncommitted changes в private area сессии)**
2. **Модифицируйте:** вынесите `abort_transaction()` в `finally` и добавьте проверку, что даже после успешного `commit` можно проверить консистентность (`A + B == 2000`).
3. **Продвинутое:** запустите *вторую* сессию параллельно, которая читает A во время паузы (используйте `threading.Event`). Что она видит — `1000` или `500`? Это и есть изоляция.

---

## Эксперимент 4: «Долговечность — journal vs checkpoint»

**Гипотеза:** `writeConcern: {w: 1, j: true}` даёт durability на уровне коммита, а `w: 0` — нет. Разница видна только при сбое.

### Скрипт `exp4_durability.py`

```python
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
import time

client = MongoClient("mongodb://localhost:27017/?replicaSet=rs0")
coll = client.lab.durability
coll.delete_many({})

# --- Коммит без journal ---
s = client.start_session()
s.start_transaction(write_concern=WriteConcern(w=1, j=False))
coll.insert_one({"_id": "no_journal", "ts": time.time()}, session=s)
s.commit_transaction()
print("Закоммичено без j=true — в журнале может ещё не быть")

# --- Коммит с journal ---
s = client.start_session()
s.start_transaction(write_concern=WriteConcern(w=1, j=True))
coll.insert_one({"_id": "with_journal", "ts": time.time()}, session=s)
s.commit_transaction()
print("Закоммичено с j=true — гарантированно в WAL")

# --- Разница видна только при краше. Покажем статус journal ---
stats = client.admin.command("serverStatus")
print("\nWiredTiger WAL stats:")
for k, v in stats["wiredTiger"]["log"].items():
    if "bytes" in k or "records" in k:
        print(f"  {k}: {v}")
```

###  Задания

1. **Эксперимент с крашем:** *(требует Docker)*
   ```bash
   # 1. Запустите exp4, но НЕ закрывайте соединение
   # 2. Внезапно убейте контейнер (без graceful shutdown):
   docker kill wt-lab
   # 3. Перезапустите и проверьте:
   docker start wt-lab
   docker exec -it wt-lab mongosh --eval "db.durability.find().toArray()"
   ```
   Оба документа выживут? Почему? *(подсказка: даже `j=false` попадёт в checkpoint через 60 сек; для чистого эксперимента нужен kill в первые секунды)*.
2. **Объясните разницу:** чем отличается durability `j=true` от durability, обеспечиваемого `checkpoint` каждые 60 секунд? Какой из них защищает от потери *последней* транзакции, а какой — от потери *всех* за последнюю минуту?
3. **Продвинутое:** настройте `mongod --setParameter enableTestCommands=1` и используйте `db.adminCommand({configureFailPoint: "crashAfter...", mode: "alwaysOn"})`, чтобы эмулировать краш сразу после `commit` без журнала. Сравните поведение.

---

## Эксперимент 5: «Наблюдение за History Store» (бонус)

**Гипотеза:** Чем дольше живёт транзакция, тем больше «мусора» копится в History Store, и это влияет на производительность.

### Скрипт `exp5_history.py`

```python
from pymongo import MongoClient
import time

client = MongoClient("mongodb://localhost:27017/?replicaSet=rs0")
db = client.lab
coll = db.history_test
coll.delete_many({})
coll.insert_one({"_id": "hot", "v": 0})

# Открываем долгую транзакцию-читателя
long_reader = client.start_session()
long_reader.start_transaction()
_ = coll.find_one({"_id": "hot"}, session=long_reader)
print("[Reader] Открыт долгий снапшот")

# Генерируем много версий документа
for i in range(1000):
    coll.update_one({"_id": "hot"}, {"$inc": {"v": 1}})

# Смотрим метрики WiredTiger
stats = db.command("serverStatus")["wiredTiger"]
cache = stats["cache"]
print(f"В кэше: {cache['bytes currently in the cache']/1e6:.1f} MB")
print(f"History Store 'dirty': {cache.get('tracked dirty bytes in the cache', 0)/1e6:.1f} MB")

long_reader.abort_transaction()  # освобождаем снапшот
time.sleep(2)
print("[Reader] Снапшот закрыт — WT может очистить историю")
```

###  Задания

1. Сравните `bytes currently in the cache` до и после закрытия долгой транзакции. Насколько выросла история?
2. **Сделайте вывод:** почему в продакшене «долгие транзакции» — это плохо для WiredTiger? Свяжите с механизмом eviction (dirty-страницы не могут быть вытеснены, пока на них ссылается старый снапшот).
3. **Провокационный вопрос:** что произойдёт, если долгая транзакция держит снапшот, а кэш переполняется? *(подсказка: `WT_CACHE_FULL` и остановка записи — это реальный инцидент, с которым сталкиваются в проде)*.

---

##  Отчёт (что сдать)

1. **Код** всех 5 экспериментов (можно в одном `.py`-файле с функциями).
2. **Логи выполнения** — вывод скриптов (скриншоты или текст) с вашими комментариями.
3. **Ответы на вопросы** под каждым экспериментом — короткие, но по существу.
4. **Мини-исследование (1 стр.):** «Почему MongoDB не позволяет транзакции длиннее 60 секунд по умолчанию?» — свяжите `transactionLifetimeLimitSeconds` с History Store, checkpoint и eviction.
5. **Вывод:** в 5–7 предложениях сформулируйте, как WiredTiger балансирует между изоляцией, параллелизмом и долговечностью.

---


