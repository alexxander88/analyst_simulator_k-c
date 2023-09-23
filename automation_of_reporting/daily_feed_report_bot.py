import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# дефолтные параметры которые прокидываются в таски
default_args = {
                'owner': 'al-serov',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2023, 9, 18)
                }

# расписание для запуска DAGa
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_aserov_daily_feed_report():

    @task
    def send_daily_feed_report(chat=-928988566):

        my_token = '####'
        bot = telegram.Bot(token=my_token) # получаем доступ к боту
        chat_id = chat or 393568442

        # создаем подключение к ClickHouse
        connection = {
                          'host': 'https://clickhouse.lab.karpov.courses',
                          'database':'simulator_20230820',
                          'user':'student',
                          'password':'dpo_python_2020'
                          }

        # запрос в ClickHouse
        q = '''
            SELECT
                toDate(time) event_date,
                uniq(user_id) DAU,
                countIf(action='view') views,
                countIf(action='like') likes,
                round(likes / views, 3) CTR
            FROM 
                {db}.feed_actions 
            WHERE 
                toDate(time) BETWEEN today()-7 AND today()-1
            GROUP BY
                event_date
            ORDER BY
                event_date DESC
            '''

        # выгружаем данные в датафрейм
        df_report = ph.read_clickhouse(q, connection=connection)

        # текст с информацией о значениях ключевых метрик за предыдущий день
        msg = (
              f'Key feed metrics for {df_report.event_date.loc[0]:%a %d %b}\n\n' 
              f'DAU: {df_report.DAU.loc[0]:,}\n'
              f'Views: {df_report.views.loc[0]:,}\n'
              f'Likes: {df_report.likes.loc[0]:,}\n'
              f'CTR: {df_report.CTR.loc[0]}'
              ).replace(',', ' ')

        # отправка текстового отчета
        bot.sendMessage(chat_id=chat_id, text=msg)

        # создание и отправка графика DAU за последние 7 дней
        sns.lineplot(data=df_report, x='event_date', y='DAU', marker ='o')
        plt.title('DAU')
        plt.xticks(rotation=15)
        plt.grid()
        plt.xlabel('')
        plt.ylabel('')
        plot_DAU = io.BytesIO()
        plt.savefig(plot_DAU)
        plot_DAU.seek(0)
        plot_DAU.name = 'DAU_lineplot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_DAU)

        # создание и отправка графика просмотров за последние 7 дней
        sns.lineplot(data=df_report, x='event_date', y='views', color='red', marker ='o')
        plt.title('Views')
        plt.xticks(rotation=15)
        plt.grid()
        plt.xlabel('')
        plt.ylabel('')
        plot_views = io.BytesIO()
        plt.savefig(plot_views)
        plot_views.seek(0)
        plot_views.name = 'views_lineplot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_views)

        # создание и отправка графика лайков за последние 7 дней
        sns.lineplot(data=df_report, x='event_date', y='likes', color='green', marker ='o')
        plt.title('Likes')
        plt.xticks(rotation=15)
        plt.grid()
        plt.xlabel('')
        plt.ylabel('')
        plot_likes = io.BytesIO()
        plt.savefig(plot_likes)
        plot_likes.seek(0)
        plot_likes.name = 'likes_lineplot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_likes)

        # создание и отправка графика CTR за последние 7 дней
        sns.lineplot(data=df_report, x='event_date', y='CTR', color='purple', marker ='o')
        plt.title('CTR')
        plt.xticks(rotation=15)
        plt.grid()
        plt.xlabel('')
        plt.ylabel('')
        plot_CTR = io.BytesIO()
        plt.savefig(plot_CTR)
        plot_CTR.seek(0)
        plot_CTR.name = 'CTR_lineplot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_CTR)
    
    send_daily_feed_report()

dag_aserov_daily_feed_report = dag_aserov_daily_feed_report()
