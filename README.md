# Платформа аналитики и прогнозирования цен на золото

Аналитическая и ML-платформа для сбора, хранения, обработки и прогнозирования цен на золото

---

## О проекте

Данный проект реализует полный цикл работы с данными:

* автоматизированный сбор исторических данных о цене золота
* построение аналитического хранилища
* обучение и сравнение нескольких ML-моделей
* хранение, версионирование и управление моделями
* автоматический перевод лучшей модели в Production
* регулярная генерация прогнозов
* визуализация данных и прогнозов в интерактивном интерфейсе

## Пример работы
  <img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/streamlit 1.png" />

  <img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/mlflow experiments chart.png" />

  <img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/superset.png" />

Проект построен по принципам:

* Data Lake + DWH
* MLOps
* Оркестрация пайплайнов
* Автоматизация жизненного цикла модели

---


# Технологический стек

| Компонент        | Технология     |
| ---------------- | -------------- |
| Оркестрация      | Apache Airflow |
| ML tracking      | MLflow         |
| Хранилище данных | PostgreSQL     |
| Аналитическая БД | ClickHouse     |
| Data Lake        | MinIO (S3)     |
| Визуализация     | Streamlit      |
| BI               | Superset       |
| Контейнеризация  | Docker         |
| Язык             | Python 3.10+   |

---

# Подробное описание реализации

## 1 Получение данных

**Источник данных:** API Центрального Банка РФ  
**Оркестрация:** Apache Airflow  

### Что реализовано:

- DAG для регулярного получения данных
- Загрузка сырья в MinIO (raw слой)
- Поддержка повторных запусков
- Разделение логики извлечение данных и загрузки
- Логирование шагов пайплайна

### Зачем:

- Обеспечивает отказоустойчивость
- Позволяет хранить сырой слой данных
- Делает пайплайн расширяемым

---

## 2 Data Lake (MinIO)

Используется S3-совместимое хранилище MinIO.

### Почему MinIO:

- Локальный S3-аналог
- Используется MLflow как artifact storage
- Позволяет реализовать озеро данных
- Удобен для контейнеризации

---

## 3 Data Warehouse (PostgreSQL)

PostgreSQL используется как:

- Хранилище аналитических таблиц
- Backend-store для MLflow
- Источник данных для Superset

### Реализовано:

- Разделение слоёв
- SQL-трансформации
- Агрегации
- Подготовка датасета для ML

### Почему PostgreSQL:

- Хорошая интеграция с BI
- Надёжность
- ACID-гарантии

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/sreamlit 2.png" />

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/pg.png" />

---

## 4 Аналитический слой (ClickHouse)

В проекте используется ClickHouse как высокопроизводительная аналитическая база данных для OLAP-нагрузки.

### Реализовано:

- Подключение ClickHouse как отдельного аналитического слоя
- Интеграция с Superset
- Агрегации
- Быстрые выборки для BI-дашбордов
- Разделение нагрузки между OLTP (Postgres) и OLAP (ClickHouse)

### Почему ClickHouse:

- Высокая скорость агрегаций
- Работа с большими объёмами данных
- Быстрые группировки и временные срезы
- Минимальная задержка ответа для визуализаций

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/streamlit 3.png" />

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/clickhouse monthly.png" />

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/clickhouse monthly 2.png" />

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/streamlit 4.png" />

---

## 5 Оркестрация (Airflow)

Airflow управляет:

- получением данных
- трансформациями
- обучением моделей
- генерацией прогнозов

### Зачем:

- Автоматизация всего пайплайна
- Масштабируемость

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/airflow dags.png" />

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/dag dependencies.png" />

---

## 6 Machine Learning

Реализован полноценный ML жизненный цикл.

### Что реализовано:

- Обучение нескольких моделей:
  - Скользящее среднее
  - SARIMAX
  - XGBoost
- Разделение train/test
- Вычисление метрик:
  - MAE
  - RMSE
  - R²
- Логирование экспериментов в MLflow
- Автоматический выбор лучшей модели
- Регистрация модели в Model Registry

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/forecast.png" />

---

## 7 MLflow (MLOps слой)

MLflow используется для:

- Отслеживание экспериментов
- Хранения артефактов
- Управления жизненным циклом

### Особенности реализации:

- Backend-store в PostgreSQL
- Artifact store в MinIO
- Автоматическая регистрация лучшей модели
- Версионирование
- Возможность отката к предыдущей версии

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/mlflow experiments.png" />

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/mlflow production.png" />

---

## 8 Прогнозирование

Отдельный DAG:

- Загружает production-модель
- Генерирует прогноз
- Сохраняет прогноз в postgres
- Делает данные доступными для BI

<img width="800" alt="Работы программы" src="https://github.com/nezlusya/gold_project/blob/main/photo/streamlit 6.png" />

---

## 9 BI слой (Superset)

Superset используется для:

- визуализации исторических данных
- отображения прогноза
- сравнения факта и предсказания
- построения аналитических дашбордов

### Реализовано:

- Подключение к PostgreSQL, Clickhouse
- Дашборд с динамикой цен
- График прогноза
- Аналитические метрики

---

# Как запустить проект

## 1. Клонировать репозиторий

```
git clone https://github.com/nezlusya/gold_project.git
cd gold_project
```

## 2. Запустить инфраструктуру

```
docker-compose up --build
docker compose up -d
```

## 3. Доступ к сервисам

| Сервис    | URL                                            |
| --------- | ---------------------------------------------- |
| Airflow   | [http://localhost:8080](http://localhost:8080) |
| MLflow    | [http://localhost:5000](http://localhost:5000) |
| Streamlit | [http://localhost:8501](http://localhost:8501) |
| Superset  | [http://localhost:8088](http://localhost:8088) |


