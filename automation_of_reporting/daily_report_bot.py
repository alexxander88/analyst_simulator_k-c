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
def dag_aserov_daily_report():

    @task
    def send_daily_report(chat=-928988566):

        my_token = '6454685986:AAFNxTHKQXPMi1WHtcdZU1QM8diVPiBg7Lc'
        bot = telegram.Bot(token=my_token) # получаем доступ к боту
        chat_id = chat or 393568442

        # создаем подключение к ClickHouse
        connection = {
                          'host': 'https://clickhouse.lab.karpov.courses',
                          'database':'simulator_20230820',
                          'user':'student',
                          'password':'dpo_python_2020'
                          }

        # запрос для ленты
        q_feed = '''
            SELECT
                toDate(time) event_date,
                uniq(user_id) DAU_feed,
                countIf(action='view') views,
                countIf(action='like') likes,
                round(likes / views, 3) CTR,
                uniq(post_id) active_posts
            FROM 
                {db}.feed_actions 
            WHERE 
                toDate(time) BETWEEN today()-8 AND today()-1
            GROUP BY
                event_date
            ORDER BY
                event_date DESC
            '''
        # запрос для новых пользователей ленты
        q_new_users_feed = '''
            SELECT 
                start_date as event_date,
                count(user_id) as new_users_feed
            FROM
                (SELECT 
                    min(day_time) start_date,
                    user_id
                FROM
                    (SELECT 
                      toStartOfDay(time) day_time,
                      user_id
                    FROM
                      {db}.feed_actions
                    )
                GROUP BY user_id)
            GROUP BY 
                event_date
            HAVING
                event_date BETWEEN today()-8 AND today()-1
            ORDER BY 
                event_date DESC
            '''

        # запрос для новых пользователей мессенджера
        q_new_users_mes = '''
            SELECT 
                start_date as event_date,
                count(user_id) as new_users_mes
            FROM
                (SELECT 
                    min(day_time) start_date,
                    user_id
                FROM
                    (SELECT 
                      toStartOfDay(time) day_time,
                      user_id
                    FROM
                      {db}.message_actions
                    )
                GROUP BY user_id)
            GROUP BY 
                event_date
            HAVING
                event_date BETWEEN today()-8 AND today()-1
            ORDER BY 
                event_date DESC
            '''

        # запрос для новых постов
        q_new_posts = '''
            SELECT 
                first_post as event_date,
                count(post_id) as new_posts
            FROM
                (SELECT 
                    min(day_time) first_post,
                    post_id
                FROM
                    (SELECT 
                      toStartOfDay(time) day_time,
                      post_id
                    FROM
                      {db}.feed_actions
                    )
                GROUP 
                    BY post_id)
            GROUP BY 
                event_date
            HAVING
                event_date BETWEEN today()-8 AND today()-1
            ORDER BY 
                event_date DESC
            '''

        # запрос для мессенджера
        q_mes = '''
            SELECT
                toDate(time) event_date,
                uniq(user_id) DAU_mes,
                count(user_id) messages_sent
            FROM 
                {db}.message_actions 
            WHERE 
                toDate(time) BETWEEN today()-8 AND today()-1
            GROUP BY
                event_date
            ORDER BY
                event_date DESC
            '''
        
        # выгружаем данные в датафреймы
        df_feed = ph.read_clickhouse(q_feed, connection=connection)
        df_new_users_feed = ph.read_clickhouse(q_new_users_feed, connection=connection)
        df_new_users_mes = ph.read_clickhouse(q_new_users_mes, connection=connection)
        df_new_posts = ph.read_clickhouse(q_new_posts, connection=connection)
        df_mes = ph.read_clickhouse(q_mes, connection=connection)
        
        # мерджим все в 1 датафрейм
        df_report = df_feed.merge(df_new_users_feed).merge(df_new_users_mes).merge(df_new_posts).merge(df_mes)
        
        # переупорядочим столбцы
        df_report = df_report[['event_date', 'DAU_feed', 'DAU_mes', 'new_users_feed', 'new_users_mes', 
                               'views', 'likes', 'CTR', 'active_posts', 'new_posts', 'messages_sent']]
        
        # текст с информацией об аудиторных данных за предыдущий день
        msg_audience = (
                        f'Audience data for {df_report.event_date.loc[0]:%a %d %b}\n\n' 

                        f'DAU feed: {df_report.DAU_feed.loc[0]:,}\n'
                        f'day ago: {df_report.DAU_feed.loc[0]/df_report.DAU_feed.loc[1]*100-100:.2f}%\n'
                        f'week ago: {df_report.DAU_feed.loc[0]/df_report.DAU_feed.loc[7]*100-100:.2f}%\n\n'

                        f'DAU messenger: {df_report.DAU_mes.loc[0]:,}\n'
                        f'day ago: {df_report.DAU_mes.loc[0]/df_report.DAU_mes.loc[1]*100-100:.2f}%\n'
                        f'week ago: {df_report.DAU_mes.loc[0]/df_report.DAU_mes.loc[7]*100-100:.2f}%\n\n'

                        f'New feed users: {df_report.new_users_feed.loc[0]:,}\n'
                        f'day ago: {df_report.new_users_feed.loc[0]/df_report.new_users_feed.loc[1]*100-100:.2f}%\n'
                        f'week ago: {df_report.new_users_feed.loc[0]/df_report.new_users_feed.loc[7]*100-100:.2f}%\n\n'

                        f'New messenger users: {df_report.new_users_mes.loc[0]:,}\n'
                        f'day ago: {df_report.new_users_mes.loc[0]/df_report.new_users_mes.loc[1]*100-100:.2f}%\n'
                        f'week ago: {df_report.new_users_mes.loc[0]/df_report.new_users_mes.loc[7]*100-100:.2f}%\n\n'
                        ).replace(',', ' ')
        
        # отправка текстового отчета по аудитории
        bot.sendMessage(chat_id=chat_id, text=msg_audience)
        
         # создание и отправка графика DAU
        sns.lineplot(data=df_report, x='event_date', y='DAU_feed', color='blue', marker='o', legend='brief', label='feed')
        sns.lineplot(data=df_report, x='event_date', y='DAU_mes', color='green', marker='o', legend='brief', label='messenger')
        plt.title('DAU')
        plt.xticks(rotation=15)
        plt.grid()
        plt.locator_params (axis='y', nbins= 10)
        plt.xlabel('')
        plt.ylabel('')
        plot_DAU = io.BytesIO()
        plt.savefig(plot_DAU)
        plot_DAU.seek(0)
        plot_DAU.name = 'DAU_lineplot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_DAU)

        #создание и отправка графика новых пользоваетелей 
        sns.lineplot(data=df_report, x='event_date', y='new_users_feed', color='blue', marker='o', legend='brief', label='feed')
        sns.lineplot(data=df_report, x='event_date', y='new_users_mes', color='green', marker='o', legend='brief', label='messenger')
        plt.title('New users')
        plt.xticks(rotation=15)
        plt.grid()
        plt.xlabel('')
        plt.ylabel('')
        plot_new_users = io.BytesIO()
        plt.savefig(plot_new_users)
        plot_new_users.seek(0)
        plot_new_users.name = 'new_users_lineplot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_new_users)

        # текст с информацией об активности пользователей за предыдущий день
        msg_activity = (
                        f'Activity data for {df_report.event_date.loc[0]:%a %d %b}\n\n' 

                        f'Views: {df_report.views.loc[0]:,}\n'
                        f'day ago: {df_report.views.loc[0]/df_report.views.loc[1]*100-100:.2f}%\n'
                        f'week ago: {df_report.views.loc[0]/df_report.views.loc[7]*100-100:.2f}%\n\n'

                        f'Likes: {df_report.likes.loc[0]:,}\n'
                        f'day ago: {df_report.likes.loc[0]/df_report.likes.loc[1]*100-100:.2f}%\n'
                        f'week ago: {df_report.likes.loc[0]/df_report.likes.loc[7]*100-100:.2f}%\n\n'

                        f'CTR: {df_report.CTR.loc[0]:,}\n'
                        f'day ago: {df_report.CTR.loc[0]/df_report.CTR.loc[1]*100-100:.2f}%\n'
                        f'week ago: {df_report.CTR.loc[0]/df_report.CTR.loc[7]*100-100:.2f}%\n\n'

                        f'Active posts: {df_report.active_posts.loc[0]:,}\n'
                        f'day ago: {df_report.active_posts.loc[0]/df_report.active_posts.loc[1]*100-100:.2f}%\n'
                        f'week ago: {df_report.active_posts.loc[0]/df_report.active_posts.loc[7]*100-100:.2f}%\n\n'

                        f'New posts: {df_report.new_posts.loc[0]:,}\n'
                        f'day ago: {df_report.new_posts.loc[0]/df_report.new_posts.loc[1]*100-100:.2f}%\n'
                        f'week ago: {df_report.new_posts.loc[0]/df_report.new_posts.loc[7]*100-100:.2f}%\n\n'

                        f'Messages sent: {df_report.messages_sent.loc[0]:,}\n'
                        f'day ago: {df_report.messages_sent.loc[0]/df_report.messages_sent.loc[1]*100-100:.2f}%\n'
                        f'week ago: {df_report.messages_sent.loc[0]/df_report.messages_sent.loc[7]*100-100:.2f}%\n\n'

                        ).replace(',', ' ')
        
        # отправка текстового отчета по активности
        bot.sendMessage(chat_id=chat_id, text=msg_activity)
        
        # создание и отправка графика просмотров, лайков
        sns.lineplot(data=df_report, x='event_date', y='views', color='red', marker='o', legend='brief', label='views')
        sns.lineplot(data=df_report, x='event_date', y='likes', color='purple', marker='o', legend='brief', label='likes')
        plt.title('Activity')
        plt.xticks(rotation=15)
        plt.grid()
        plt.locator_params (axis='y', nbins= 10)
        plt.xlabel('')
        plt.ylabel('')
        plot_activity = io.BytesIO()
        plt.savefig(plot_activity)
        plot_activity.seek(0)
        plot_activity.name = 'activity_lineplot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_activity)
    
        # создание и отправка графика CTR
        sns.lineplot(data=df_report, x='event_date', y='CTR', color='black', marker='o')
        plt.title('CTR')
        plt.xticks(rotation=15)
        plt.grid()
        plt.locator_params (axis='y', nbins= 10)
        plt.xlabel('')
        plt.ylabel('')
        plot_CTR = io.BytesIO()
        plt.savefig(plot_CTR)
        plot_CTR.seek(0)
        plot_CTR.name = 'CTR_lineplot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_CTR)

        # отправка файла
        file_object = io.StringIO()
        df_report.to_csv(file_object)
        file_object.name = 'Data for the previous 8 days.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)

            
    send_daily_report()

dag_aserov_daily_report = dag_aserov_daily_report()