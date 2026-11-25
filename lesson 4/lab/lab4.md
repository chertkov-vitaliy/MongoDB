## Цель работы

Путем последовательной «переделки» одного и того же запроса заставить планировщик MongoDB пройти путь от худшего плана (`COLLSCAN` + `SORT` в памяти) до лучшего (`IXSCAN` без `SORT`, в идеале — покрывающий запрос), и научиться количественно оценивать каждый шаг оптимизации через метрики `explain("executionStats")`.

Ключевой критерий: чем ближе `totalDocsExamined` к `nReturned` и чем меньше лишних стадий (`SORT`, `FETCH`), тем лучше план.

---

## Оборудование и ПО

- MongoDB Community Server 6.0+ (совместимо с 5.0+)
- `mongosh`
- Коллекция `lab_query_engine.products` (генерируется ниже)

---

## Запуск контейнера с именем replica set "rs0"
```javascript 
docker run --rm -d -p 27017:27017 -h $(hostname) --name mongo-lab mongo:8.0 --replSet=rs0
```
---

## Инициализация replica set (с небольшой задержкой, чтобы контейнер успел запуститься)
```javascript
docker exec mongo-lab mongosh --quiet --eval "rs.initiate();"
```

## Подготовка данных

Выполните в `mongosh`. Объем — **200 000 документов**, чтобы разница между `COLLSCAN` и `IXSCAN` была измеримой:

```javascript
use lab_query_engine
db.products.drop()

const categories = ["electronics", "books", "clothing", "food", "toys"]
const brands = ["alpha", "beta", "gamma", "delta"]
const batch = []

for (let i = 0; i < 200000; i++) {
  batch.push({
    sku: "SKU-" + i,
    category: categories[i % categories.length],
    brand: brands[i % brands.length],
    price: Math.floor(Math.random() * 100000) / 100,
    stock: Math.floor(Math.random() * 500),
    createdAt: new Date(Date.now() - Math.floor(Math.random() * 730) * 86400000)
  })
}

db.products.insertMany(batch)
print("Документов:", db.products.countDocuments())
```

**Целевой запрос** — вокруг него строится весь эксперимент:

```javascript
db.products.find({
  category: "electronics",
  stock: { $lt: 50 }
}).sort({ price: -1 }).limit(20)
```

Найти электронику с дефицитом склада (меньше 50), отсортировать по цене по убыванию, взять первые 20.

---

## Этап 1. Зафиксировать «худший» план (точка отсчета)

На этом этапе индексов нет вообще — только автоматический `_id_`.

```javascript
const e0 = db.products.explain("executionStats").find({
  category: "electronics",
  stock: { $lt: 50 }
}).sort({ price: -1 }).limit(20)
```

**Задание 1.1.** Заполните первую строку таблицы (см. раздел «Форма отчета») следующими значениями из `e0`:

- `winningPlan.stage` (верхняя стадия);
- есть ли в дереве плана стадии `SORT`, `COLLSCAN`, `IXSCAN`, `FETCH`;
- `executionStats.totalDocsExamined`;
- `executionStats.nReturned`;
- `executionStats.executionTimeMillis`.

**Задание 1.2.** Письменно ответьте:

1. Почему `totalDocsExamined` во много раз больше `nReturned`?
2. Какая стадия выполняет сортировку и почему она «блокирующая» (то есть почему нельзя отдать первые 20 документов, не прочитав все подходящие)?
3. Ограничение `limit(20)` помогло сократить `totalDocsExamined`? Объясните.

---

## Этап 2. Один простой индекс — первый прогресс

Создайте индекс только по `category`:

```javascript
db.products.createIndex({ category: 1 })
```

Повторите `explain("executionStats")` тем же запросом.

**Задание 2.1.** Заполните вторую строку таблицы и ответьте:

1. Изменилась ли верхняя стадия (`COLLSCAN` → `IXSCAN`)?
2. Как изменились `totalDocsExamined` и `totalKeysExamined`? Почему `totalKeysExamined` теперь больше нуля?
3. Исчезла ли стадия `SORT`? Почему нет — ведь индекс по `category` не задает порядок по `price`?

**Вывод этапа:** индекс ускорил фильтрацию, но сортировка по-прежнему в памяти — план все еще далек от идеала.

---

## Этап 3. Составной индекс «фильтр + сортировка»

Удалите предыдущий индекс и создайте составной, где порядок полей соответствует фильтру и сортировке:

```javascript
db.products.dropIndex({ category: 1 })
db.products.createIndex({ category: 1, price: -1 })
```

Повторите `explain("executionStats")`.

**Задание 3.1.** Заполните третью строку таблицы и ответьте:

1. Исчезла ли стадия `SORT` из плана? Почему индекс `{category:1, price:-1}` обеспечивает нужный порядок?
2. Сравните `executionTimeMillis` с этапом 2. Что дало больше эффекта — ускорение фильтра или устранение `SORT`?
3. Осталась ли стадия `FETCH`? Почему она есть, хотя индекс уже хороший?

**Важно:** порядок полей в составном индексе критичен. Индекс `{price:-1, category:1}` не устранил бы `SORT` для этого запроса, потому что фильтр по `category` не является префиксом.

---

## Этап 4. Довести до покрывающего запроса

Измените запрос так, чтобы из него убрать `_id` из проекции, и создайте индекс, включающий все нужные поля:

```javascript
db.products.dropIndex({ category: 1, price: -1 })
db.products.createIndex({ category: 1, price: -1, stock: 1 })
```

Запрос с проекцией, покрываемой индексом:

```javascript
const e3 = db.products.explain("executionStats").find(
  { category: "electronics", stock: { $lt: 50 } },
  { _id: 0, category: 1, price: 1, stock: 1 }
).sort({ price: -1 }).limit(20)
```

**Задание 4.1.** Заполните четвертую строку таблицы и ответьте:

1. Присутствует ли стадия `FETCH` в `winningPlan`? Если нет — запрос стал **покрывающим** (covered query).
2. Чему равен `totalDocsExamined` у покрывающего запроса? Объясните, почему именно так.
3. Сравните `executionTimeMillis` с этапом 3.

**Замечание:** индекс `{category:1, price:-1, stock:1}` может обслуживать и фильтр, и сортировку, и проекцию. Но `stock` в фильтре используется как диапазон — это нормально для неведущего поля составного индекса (правило ESR: Equality → Sort → Range).

---

## Этап 5. Проверка правила ESR и «неудачного» порядка полей

«Не любой индекс — удачный», проведите контролируемый эксперимент.

```javascript
db.products.dropIndex({ category: 1, price: -1, stock: 1 })
db.products.createIndex({ stock: 1, category: 1, price: -1 })
```

Повторите `explain("executionStats")` тем же (покрывающим) запросом.

**Задание 5.1.** Заполните пятую строку таблицы и ответьте:

1. Вернулась ли стадия `SORT`? Почему индекс с ведущим полем `stock` (диапазон) хуже индекса с ведущим `category` (равенство)?
2. Как изменились `totalKeysExamined` и `totalDocsExamined`?
3. Сформулируйте правило ESR (Equality → Sort → Range) своими словами и объясните, как оно проявляется в этом эксперименте.

---

## Этап 6. Анализ кэша планов (опционально, для сильных групп)

```javascript
db.products.getPlanCache().clear()

// Два запроса одной формы, разных значений
db.products.find({ category: "electronics", stock: { $lt: 50 } }).explain("executionStats")
db.products.find({ category: "books", stock: { $lt: 100 } }).explain("executionStats")

db.products.aggregate([{ $planCacheStats: {} }])
```

**Задание 6.1.** Найдите `planCacheKey`. Объясните, почему два запроса с разными значениями имеют один ключ, и что произойдет с ключом после создания нового индекса.

---

## Форма отчета

### Таблица результатов (обязательна)

| Этап | Индекс(ы) | Верхняя стадия | Есть SORT? | Есть FETCH? | totalKeysExamined | totalDocsExamined | nReturned | Время, мс |
|---|---|---|---|---|---|---|---|---|
| 1. Без индексов | — | | | | | | | |
| 2. Индекс по category | `{category:1}` | | | | | | | |
| 3. Составной | `{category:1, price:-1}` | | | | | | | |
| 4. Покрывающий | `{category:1, price:-1, stock:1}` | | | | | | | |
| 5. «Неудачный» порядок | `{stock:1, category:1, price:-1}` | | | | | | | |

### Текстовые разделы отчета

1. Заполненная таблица.
2. Ответы на задания 1.1–6.1 письменно.
3. **Итоговый вывод:** какие три-четыре правила проектирования индексов вы вывели из эксперимента.

---

## Контрольные вопросы

1. Почему `limit(20)` сам по себе не спасает от `COLLSCAN`, если нет индекса, поддерживающего сортировку?
2. В чем разница между `totalKeysExamined` и `totalDocsExamined`? В каком случае второе равно нулю?
3. Почему индекс `{category:1, price:-1}` устраняет `SORT`, а `{price:-1, category:1}` — нет?
4. Сформулируйте правило ESR и приведите пример индекса, который его нарушает для запроса из этапа 4.
5. Что произойдет с планом, если после этапа 5 создать оба индекса одновременно? Как планировщик выберет победителя?


## Рекомендуемая литература

1. MongoDB Documentation. *Explain Results*.
2. MongoDB Documentation. *Query Plans and the Plan Cache*.
3. MongoDB Documentation. *Indexes: Compound Indexes and the ESR Guideline*.
4. MongoDB Documentation. *Covered Queries*.