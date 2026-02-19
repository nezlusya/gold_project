import streamlit as st
import pandas as pd
import plotly.express as px
import clickhouse_connect
import psycopg2
import logging
import os


# -----------------------------
# Настройки подключения
# -----------------------------
POSTGRES_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "database": os.getenv("PG_DB"),
}

CLICKHOUSE_CONFIG = {
    "host": "clickhouse",
    "user": "default",
    "password": ""
}

# -----------------------------
# Загрузка данных
# -----------------------------
def load_postgres_data():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    df = pd.read_sql("SELECT * FROM dm.gold_change_day ORDER BY date", conn)
    conn.close()
    return df

def load_clickhouse_data():
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    df = client.query_df("SELECT * FROM gold_price_cbr ORDER BY date")
    return df

def load_clickhouse_monthly_dm():
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    df = client.query_df("SELECT * FROM dm_gold_price_monthly ORDER BY month")
    return df

def load_forecast_data():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    df = pd.read_sql("""
        SELECT date, forecast
        FROM cdm.gold_forecast
        ORDER BY date
    """, conn)
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df

# -----------------------------
# Интерфейс
# -----------------------------
st.set_page_config(page_title="Gold Analytics", layout="wide")
st.title("🏆 Аналитическая система цен на золото")

# Ссылки на внешние системы
with st.sidebar:
    st.header("🔗 Ссылки")

    st.markdown("### 🗄️ Хранилище сырых данных")
    st.markdown("[MinIO UI](http://localhost:9001)")

    st.markdown("### 🧩 Оркестрация процессов")
    st.markdown("[Airflow UI](http://localhost:8081)")

    st.markdown("### 📊 BI-аналитика")
    st.markdown("[Superset UI](http://localhost:8088)")

    st.markdown("### 👀 Отслеживание экспериментов")
    st.markdown("[MLflow](http://localhost:8501)")


# Выбор вкладок
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Последние 6 месяцев (PostgreSQL)",
    "📈 История с 1998 года (ClickHouse)",
    "📅 Месячная витрина DM (ClickHouse)",
    "🔮 Прогноз цены золота (ML)"
])

# ------------------------------------------------------------
# TAB 1 — PostgreSQL (6 months)
# ------------------------------------------------------------
with tab1:
    st.header("📊 Данные за последние 6 месяцев (PostgreSQL)")
    df_pg = load_postgres_data()

    if df_pg.empty:
        st.warning("Нет данных в PostgreSQL.")
    else:
        # Метрики
        col1, col2 = st.columns(2)
        col1.metric("Средняя цена", round(df_pg["buy_price"].mean(), 2))
        col2.metric("Количество дней", len(df_pg))

        # Графики
        fig = px.line(df_pg, x="date", y=["buy_price", "sell_price"], title="Стоимость золота (6 месяцев)")
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.area(df_pg, x="date", y="buy_change_pct", title="Разница в стоимости с предыдущим днем")
        st.plotly_chart(fig2, use_container_width=True)

        st.subheader("📄 Таблица данных")
        st.dataframe(df_pg, use_container_width=True)

# ------------------------------------------------------------
# TAB 2 — ClickHouse Full History
# ------------------------------------------------------------
with tab2:
    st.header("📈 Исторические данные с 1998 года (ClickHouse)")
    df_ch = load_clickhouse_data()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Минимальная цена", round(df_ch["buy_price"].min(), 2))
    col2.metric("Mаксимальная цена", round(df_ch["buy_price"].max(), 2))
    col3.metric("Средняя цена", round(df_ch["buy_price"].mean(), 2))
    col4.metric("Волатильность", round(df_ch["buy_price"].std(), 2))

    # Фильтр по датам
    min_date = pd.to_datetime(df_ch["date"].min()).to_pydatetime()
    max_date = pd.to_datetime(df_ch["date"].max()).to_pydatetime()
    date_range = st.slider("Выберите период", min_value=min_date, max_value=max_date, value=(min_date, max_date))

    df_filtered = df_ch[(df_ch["date"] >= date_range[0]) & (df_ch["date"] <= date_range[1])]
    fig3 = px.line(df_filtered, x="date", y=["buy_price", "sell_price"], title="Цена золота за выбранный период")
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("📄 Таблица данных")
    st.dataframe(df_filtered, use_container_width=True)

# ------------------------------------------------------------
# TAB 3 — DM Gold Price Monthly
# ------------------------------------------------------------
with tab3:
    st.header("📅 Месячная витрина dm_gold_price_monthly (ClickHouse)")
    df_dm = load_clickhouse_monthly_dm()

    if df_dm.empty:
        st.warning("Нет данных в витрине DM.")
    else:
        st.write("### Выберите метрику для графика")
        metric = st.selectbox(
            "Метрика",
            ["avg_buy_price", "avg_sell_price", "avg_spread", "volatility", "max_buy_price", "min_buy_price"],
            format_func=lambda x: {
                "avg_buy_price": "Средняя цена покупки",
                "avg_sell_price": "Средняя цена продажи",
                "avg_spread": "Средний разброс",
                "volatility": "Волатильность",
                "max_buy_price": "Максимальная цена покупки",
                "min_buy_price": "Минимальная цена покупки"
            }[x]
        )

        fig = px.line(df_dm, x="month", y=metric, title=f"График: {metric}", markers=True)
        st.plotly_chart(fig, use_container_width=True)

        # Сравнение покупки и продажи
        fig2 = px.line(df_dm, x="month", y=["avg_buy_price", "avg_sell_price"], title="Средняя покупка vs продажа")
        st.plotly_chart(fig2, use_container_width=True)

        st.subheader("📄 Таблица DM")
        st.dataframe(df_dm, use_container_width=True)

# ------------------------------------------------------------
# TAB 4 — ML Forecast
# ------------------------------------------------------------
with tab4:

    st.header("🔮 Прогноз цены золота (Production ML model)")

    df_fact = load_postgres_data()
    df_forecast = load_forecast_data()

    if df_forecast.empty:
        st.warning("Нет прогнозов. Запусти DAG ml_predict_gold_6m")
    else:

        # последние фактические значения
        last_fact = df_fact[["date", "buy_price"]].copy()
        last_fact["type"] = "Факт"

        forecast = df_forecast.copy()
        forecast.rename(columns={"forecast": "buy_price"}, inplace=True)
        forecast["type"] = "Прогноз"

        combined = pd.concat([last_fact, forecast])

        # метрики
        col1, col2, col3 = st.columns(3)

        col1.metric(
            "Последняя фактическая цена",
            round(last_fact["buy_price"].iloc[-1], 2)
        )

        col2.metric(
            "Прогноз на завтра",
            round(forecast["buy_price"].iloc[0], 2)
        )

        col3.metric(
            "Прогноз через 14 дней",
            round(forecast["buy_price"].iloc[-1], 2)
        )

        # основной график
        fig = px.line(
            combined,
            x="date",
            y="buy_price",
            color="type",
            title="Фактическая цена и прогноз (Production model)",
            markers=True
        )

        st.plotly_chart(fig, use_container_width=True)

        # таблица прогнозов
        st.subheader("📄 Таблица прогнозов")

        st.dataframe(
            df_forecast,
            use_container_width=True
        )

# -----------------------------
# Конец приложения
# -----------------------------
st.markdown("---")
st.markdown("💡 Приложение позволяет быстро анализировать цены на золото, с визуализацией последних 6 месяцев, полной историей и агрегированной витриной DM.")
