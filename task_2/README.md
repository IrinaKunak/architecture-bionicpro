Запуск
- Требуется docker и docker-compose.
- В корне: docker-compose up -d.
- Фронтенд: http://localhost:3000
- Бэкенд: http://localhost:8000
- Keycloak: http://localhost:8080

Использование
- Открыть фронт по адресу http://localhost:3000.
- Залогиниться через Keycloak.
- При необходимости выбрать from_date и to_date (YYYY-MM-DD). Пусто = последние 7 дней.
- Нажать Get Report. Таблица покажет агрегаты за выбранный период.
- Протестировать выход - кнопка Logout для выхода.

Тестовые данные
- Лежат в CSV: ./airflow/data/crm_clients.csv и ./airflow/data/telemetry.csv.
- Скрипт генерации тестовых данных airflow/generate_test_data.py

Пользователи в Keycloak
- Готовый экспорт realm уже подключен: ./keycloak/realm-export.json.
- Если нужно руками: можно зайти на http://localhost:8080, realm reports-realm, добавить пользователя и выдать роль стандартно через UI.

Обновление данных в Airflow
- Dags в ./airflow/dags, пример загрузки в bionic_reports_dag.py.
- Добавить задачу: положить SQL или Python-скрипт в dags и описать оператор в DAG (см. build_mart_ch в bionic_reports_dag.py).
- Плановое обновление: выставить schedule_interval в DAG, либо вручную через UI Airflow (http://localhost:8081 по умолчанию).

