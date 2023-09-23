import pandas as pd
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import io
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# создаем подключение к ClickHouse
connection = {
                  'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'simulator_20230820',
                  'user':'student',
                  'password':'dpo_python_2020'
                  }

# дефолтные параметры которые прокидываются в таски
default_args = {
                'owner': 'al-serov',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2023, 9, 18)
                }

# расписание для запуска DAGa
schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_aserov_alert_system():
    
    @task
    def extract_feed(connection=connection):
        q_feed = '''
            SELECT
                toStartOfFifteenMinutes(time) as fifteen_minutes,
                toDate(fifteen_minutes) as date,
                formatDateTime(fifteen_minutes, '%R') as hour_minutes,
                uniqExact(user_id) as DAU_feed,
                countIf(user_id, action='view') Views,
                countIf(user_id, action='like') Likes
            FROM 
                {db}.feed_actions 
            WHERE
                time >= today() - 1 and time < toStartOfFifteenMinutes(now())
            GROUP BY
                fifteen_minutes, date, hour_minutes
            ORDER BY
                fifteen_minutes
            '''

        return ph.read_clickhouse(q_feed, connection=connection)
    
    @task
    def extract_messenger(connection=connection):
        q_mes = '''
            SELECT
                toStartOfFifteenMinutes(time) as fifteen_minutes,
                toDate(fifteen_minutes) as date,
                formatDateTime(fifteen_minutes, '%R') as hour_minutes,
                uniqExact(user_id) as DAU_messenger,
                count(user_id) as Messages
            FROM 
                {db}.message_actions 
             WHERE
                time >= today() - 1 and time < toStartOfFifteenMinutes(now())
            GROUP BY
                fifteen_minutes, date, hour_minutes
            ORDER BY
                fifteen_minutes
            '''

        return ph.read_clickhouse(q_mes, connection=connection)
    
    @task
    def extract_merge(df_feed, df_messenger):
        return df_feed.merge(df_messenger)

    @task
    # функция для отправки алёрта в телеграмм
    def run_alerts(data, chat_id=None):
        chat_id = chat_id or 393568442
        bot = telegram.Bot(token='6400805550:AAGeDEEDTyhY0tJBEF32BGVoHofXMYjJbqs')

        # функция для проверки наличия аномалий
        def check_anomaly(df, metric, a=4, n=5):
            df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25) # 25-й квантиль, параметр n - кол-во предыдущих периодов для расчета
            df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75) # 75-й квантиль
            df['iqr'] = df['q75'] - df['q25'] # межквартильный размах
            df['up'] = df['q75'] + a * df['iqr']
            df['low'] = df['q25'] - a * df['iqr']

            df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
            df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

            if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
                is_alert = 1
            else:
                is_alert = 0

            return is_alert, df

        metrics = ['DAU_feed', 'Views', 'Likes', 'DAU_messenger', 'Messages']

        for metric in metrics:
            print(metric)
            df = data[['fifteen_minutes', 'date', 'hour_minutes', metric]].copy()
            is_alert, df = check_anomaly(df, metric)

            if is_alert:
                diff = df[metric].iloc[-1]/df[metric].iloc[-2]*100-100
                icon = ''
                
                if diff > 0:
                    icon = '⬆️'
                elif diff < 0:
                    icon = '⬇️'
                
                msg = (
                       f'{df.hour_minutes.iloc[-2]} - {df.hour_minutes.iloc[-1]}\n\n'
                       f'{metric}: {df[metric].iloc[-1]:,}\n\n'
                       f'period ago: {diff:.2f}% {icon}\n\n'
                       f'dashboard:\nhttps://clck.ru/35ookb'
                      ).replace(',', ' ')

                sns.set(rc={'figure.figsize': (16, 10)})
                plt.tight_layout()

                ax = sns.lineplot(x=df['fifteen_minutes'], y=df[metric], label='metric')
                ax = sns.lineplot(x=df['fifteen_minutes'], y=df['up'], label='up')
                ax = sns.lineplot(x=df['fifteen_minutes'], y=df['low'], label='low')
                
                # убираем подписи осей, в этом случае график понятен и без них
                ax.set(xlabel='')
                ax.set(ylabel='')

                ax.set_title(f'{metric}') # задаем заголовок графика
                ax.set(ylim=(0, None)) # задаем лимит для оси У

                # формируем файловый объект
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f'{metric}.png'
                plt.close()

                # отправляем алерт
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df_feed = extract_feed()
    df_messenger = extract_messenger()
    data = extract_merge(df_feed, df_messenger)
    run_alerts(data, chat_id=-968261538)
        
dag_aserov_alert_system = dag_aserov_alert_system()
