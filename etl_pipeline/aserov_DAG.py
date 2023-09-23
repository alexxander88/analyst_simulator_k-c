from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task

connection_sim = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20230820',
    'user': 'student',
    'password': 'dpo_python_2020'
}

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'test',
    'user': 'student-rw',
    'password': '656e2b0c9c'
}

default_args = {
    'owner': 'al-serov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 17)
}

yesterday = datetime.today() - timedelta(days=1)
yesterday = yesterday.strftime("%Y/%m/%d")

schedule_interval = '0 03 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_aserov():
    @task
    def extract_feed_actions(connection=connection_sim):
        q = '''
        SELECT
            user_id,
            gender,
            age,
            os,
            countIf(action='view') views,
            countIf(action='like') likes

        FROM 
            {db}.feed_actions 
        WHERE 
            toDate(time) = today() - 1
        GROUP BY
            user_id,
            gender,
            age,
            os
        '''
        df_feed = ph.read_clickhouse(q, connection=connection_sim)
        return df_feed

    @task
    def extract_message_actions(connection=connection_sim):
        q = '''
        WITH 
        all_users as 
            (
            SELECT DISTINCT user_id, gender, age, os
            FROM simulator_20230820.feed_actions
            UNION ALL
            SELECT DISTINCT user_id, gender, age, os
            FROM simulator_20230820.message_actions
                ),
        message_senders_and_users_receivers as
            (
            SELECT distinct user_id, messages_sent, users_sent, messages_received, users_received
            FROM
            (
            SELECT user_id, count(reciever_id) messages_sent, count(distinct reciever_id) users_sent
            FROM simulator_20230820.message_actions
            WHERE toDate(time) = today() - 1
            GROUP BY user_id
            ) message_senders

            FULL JOIN

            (
            SELECT reciever_id as user_id, count(user_id) messages_received, count(distinct user_id) users_received
            FROM simulator_20230820.message_actions
            WHERE toDate(time) = today() - 1
            GROUP BY user_id
            ) message_receivers USING(user_id)
                )

        SELECT
            distinct user_id,
            gender,
            age,
            os,
            messages_received,
            messages_sent,
            users_received,
            users_sent            
        FROM
            message_senders_and_users_receivers
            LEFT JOIN all_users USING(user_id)

        '''
        df_message = ph.read_clickhouse(q, connection=connection_sim)
        return df_message

    @task
    def extract_merge(df_feed, df_message):
        df_merge = df_feed.merge(df_message \
                                 , on=('user_id', 'gender', 'age', 'os') \
                                 , how='outer', suffixes=('_x', '_y')) \
            .fillna(value=0)
        return df_merge

    @task
    def transform_gender(df_merge):
        df_gender = df_merge.drop(['user_id', 'age', 'os'], axis=1).groupby('gender', as_index=False).sum()
        df_gender.rename(columns={'gender': 'dimension_value'}, inplace=True)
        df_gender.insert(loc=0, column='dimension', value='gender')
        df_gender.insert(loc=0, column='event_date', value=yesterday)
        return df_gender

    @task
    def transform_age(df_merge):
        df_age = df_merge.drop(['user_id', 'gender', 'os'], axis=1).groupby('age', as_index=False).sum()
        df_age.rename(columns={'age': 'dimension_value'}, inplace=True)
        df_age.insert(loc=0, column='dimension', value='age')
        df_age.insert(loc=0, column='event_date', value=yesterday)
        return df_age

    @task
    def transform_os(df_merge):
        df_os = df_merge.drop(['user_id', 'gender', 'age'], axis=1).groupby('os', as_index=False).sum()
        df_os.rename(columns={'os': 'dimension_value'}, inplace=True)
        df_os.insert(loc=0, column='dimension', value='os')
        df_os.insert(loc=0, column='event_date', value=yesterday)
        return df_os

    @task
    def transform_concat(df_gender, df_age, df_os):
        df_result = pd.concat([df_gender, df_age, df_os], ignore_index=True) \
            .astype(
            {
                'event_date': 'datetime64[ns]',
                'views': 'int64',
                'likes': 'int64',
                'messages_received': 'int64',
                'messages_sent': 'int64',
                'users_received': 'int64',
                'users_sent': 'int64'
            }
        )

        return df_result

    @task
    def load_to_ch(df_result):
        query_load = """CREATE TABLE IF NOT EXISTS test.aserov
            (
                event_date DATE,
                dimension String,
                dimension_value String,
                views UInt64,
                likes UInt64,
                messages_received UInt64,
                messages_sent UInt64,
                users_received UInt64,
                users_sent UInt64
            ) 
            ENGINE = MergeTree()
            ORDER BY event_date;
            """

        ph.execute(query=query_load, connection=connection_test)
        ph.to_clickhouse(df=df_result, table='aserov', connection=connection_test, index=False)

    df_feed = extract_feed_actions()
    df_message = extract_message_actions()
    df_merge = extract_merge(df_feed, df_message)
    df_gender = transform_gender(df_merge)
    df_age = transform_age(df_merge)
    df_os = transform_os(df_merge)
    df_result = transform_concat(df_age, df_gender, df_os)
    load_to_ch(df_result)


dag_aserov = dag_aserov()