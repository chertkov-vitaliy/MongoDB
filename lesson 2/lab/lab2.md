# Лабораторная работа: Развертывание шардированного кластера MongoDB из двух шардов в Docker

## Цель работы

На практике освоить развертывание шардированного кластера MongoDB с использованием Docker Compose, понять взаимодействие трех основных компонентов: сервера конфигурации (Config Server), шардов (Shard) и маршрутизатора (Mongos), а также освоить базовые операции шардирования коллекций и проверки распределения данных.

## Предварительные знания

- Базовое использование командной строки Linux
- Базовые концепции Docker и Docker Compose
- Базовый синтаксис MongoDB (`mongosh`)

##  Архитектура эксперимента

В данной лабораторной работе будет развернут минимальный, но полнофункциональный шардированный кластер, включающий:

| Компонент | Количество | Роль |
|-----------|------------|------|
| Config Server | 1 | Хранит метаданные кластера (в производственной среде рекомендуется развертывать как набор реплик из 3 узлов, в данной работе для упрощения используется 1 узел) |
| Shard | 2 | Каждый шард развертывается как одноузловой набор реплик (Replica Set), хранение фактических данных |
| Mongos | 1 | Маршрутизатор запросов, единая точка входа для приложений |

> **Примечание**: В производственной среде все шарды и серверы конфигурации должны быть развернуты как наборы реплик (Replica Set) для обеспечения высокой доступности. Данная лабораторная работа для демонстрации принципов работы и экономии ресурсов использует одноузловые наборы реплик.

## Структура проекта

Создайте следующую структуру каталогов:

```
mongo-sharding-lab/
├── docker-compose.yml
└── init/
    ├── init-config.js
    ├── init-shard1.js
    ├── init-shard2.js
    └── add-shards.js
```

## Часть первая: Написание файла Docker Compose

Создайте файл `docker-compose.yml`:

```yaml
version: '3.8'

services:
  # Сервер конфигурации
  configsvr:
    image: mongo:8.0
    container_name: mongo-configsvr
    command: mongod --configsvr --replSet configRS --port 27017 --bind_ip_all
    ports:
      - "27017:27017"
    networks:
      - mongo-shard-net
    volumes:
      - config-data:/data/db

  # Шард 1
  shard1:
    image: mongo:8.0
    container_name: mongo-shard1
    command: mongod --shardsvr --replSet shard1RS --port 27018 --bind_ip_all
    ports:
      - "27018:27018"
    networks:
      - mongo-shard-net
    volumes:
      - shard1-data:/data/db

  # Шард 2
  shard2:
    image: mongo:8.0
    container_name: mongo-shard2
    command: mongod --shardsvr --replSet shard2RS --port 27019 --bind_ip_all
    ports:
      - "27019:27019"
    networks:
      - mongo-shard-net
    volumes:
      - shard2-data:/data/db

  # Маршрутизатор Mongos
  mongos:
    image: mongo:8.0
    container_name: mongo-mongos
    command: mongos --configdb configRS/configsvr:27017 --port 27020 --bind_ip_all
    ports:
      - "27020:27020"
    networks:
      - mongo-shard-net
    depends_on:
      - configsvr
      - shard1
      - shard2

networks:
  mongo-shard-net:
    driver: bridge

volumes:
  config-data:
  shard1-data:
  shard2-data:
```

**Ключевые моменты для понимания**:
- `--configsvr`: указывает, что данный экземпляр является сервером конфигурации
- `--shardsvr`: указывает, что данный экземпляр является шардом
- `--replSet`: указывает имя набора реплик
- `mongos --configdb`: указывает адрес сервера конфигурации, к которому подключается Mongos

## Часть вторая: Инициализация кластера

### Шаг 1: Запуск контейнеров

```bash
cd mongo-sharding-lab
docker-compose up -d
```

Подождите несколько секунд, пока все контейнеры запустятся. Проверьте статус:

```bash
docker-compose ps
```

### Шаг 2: Инициализация набора реплик сервера конфигурации

Войдите в контейнер сервера конфигурации:

```bash
docker exec -it mongo-configsvr mongosh --port 27017
```

Выполните в оболочке mongosh:

```javascript
rs.initiate({
  _id: "configRS",
  configsvr: true,
  members: [
    { _id: 0, host: "configsvr:27017" }
  ]
})
```

После выполнения команды `exit` выйдите из оболочки.

### Шаг 3: Инициализация набора реплик шарда 1

```bash
docker exec -it mongo-shard1 mongosh --port 27018
```

```javascript
rs.initiate({
  _id: "shard1RS",
  members: [
    { _id: 0, host: "shard1:27018" }
  ]
})
```

После выполнения выйдите.

### Шаг 4: Инициализация набора реплик шарда 2

```bash
docker exec -it mongo-shard2 mongosh --port 27019
```

```javascript
rs.initiate({
  _id: "shard2RS",
  members: [
    { _id: 0, host: "shard2:27019" }
  ]
})
```

После выполнения выйдите.

### Шаг 5: Добавление шардов в кластер через Mongos

Войдите в контейнер Mongos:

```bash
docker exec -it mongo-mongos mongosh --port 27020
```

Добавьте два шарда:

```javascript
sh.addShard("shard1RS/shard1:27018")
sh.addShard("shard2RS/shard2:27019")
```

Проверьте статус кластера:

```javascript
sh.status()
```

Вы должны увидеть два шарда и один сервер конфигурации в выводе, что означает успешное создание кластера.

## Часть третья: Эксперимент с шардированием коллекции

### Шаг 1: Создание данных и включение шардирования

Оставаясь в оболочке mongosh Mongos, выполните:

```javascript
// Создание базы данных и коллекции
use labdb
db.users.insertMany([
  { userId: 1, name: "Alice", region: "US" },
  { userId: 2, name: "Bob", region: "EU" },
  { userId: 3, name: "Charlie", region: "US" },
  { userId: 4, name: "Diana", region: "ASIA" },
  { userId: 5, name: "Eve", region: "EU" }
])

// Включение шардирования базы данных (в MongoDB 6.0+ можно пропустить, команда shardCollection выполнит автоматически)
sh.enableSharding("labdb")

// Шардирование коллекции с использованием хешированного ключа шардирования по userId
sh.shardCollection("labdb.users", { userId: "hashed" })
```

### Шаг 2: Проверка распределения данных

```javascript
// Просмотр распределения данных по шардам
db.users.getShardDistribution()
```

Вы должны увидеть, что документы распределены между `shard1RS` и `shard2RS`. Поскольку количество данных невелико, возможно, все они окажутся в одном чанке, но это нормально. В реальной среде балансировщик постепенно выровняет распределение.

### Шаг 3: Проверка целевых запросов

```javascript
// Запрос с ключом шардирования (целевой запрос, Mongos напрямую направляет на соответствующий шард)
db.users.find({ userId: 2 })

// Используйте explain("executionStats") для получения подробной информации о плане выполнения
const result = db.users.find({ userId: 2 }).explain("executionStats")

// Распечатайте выигравший план (winningPlan)
printjson(result.queryPlanner.winningPlan)

// Запрос без ключа шардирования (запрос с широковещательной рассылкой, Mongos опрашивает все шарды)
db.users.find({ region: "US" })
```

##  Вопросы для размышления

1.  **Почему в данной лабораторной работе каждый шард развернут как одноузловой набор реплик, какое архитектурное требование MongoDB?** 
2.  **Если изменить ключ шардирования с `{ userId: "hashed" }` на `{ userId: 1 }` (диапазонное шардирование), что произойдет при массовой вставке монотонно возрастающих userId?** 
3.  **В чем разница между `sh.status()` и `db.users.getShardDistribution()`? Какие аспекты состояния кластера они соответственно показывают?**
4.  **Чтобы вызвать автоматическую миграцию балансировщика в MongoDB 8, ключевой момент — создать достаточно большую разницу в объёме данных между двумя шардами. В версиях 6.0 и выше балансировщик ориентируется на размер данных, а не на количество чанков, и порог миграции обычно составляет 384 MB (3 × размер чанка по умолчанию 128 MB). Сгенерируйте программу массовой вставке данных в коллекцию для создания дисбаланса, достаточного для запуска балансировщика.**


## Очистка (по завершении эксперимента)

```bash
docker-compose down -v
```

Параметр `-v` удалит тома данных, обеспечивая полную очистку.

---
