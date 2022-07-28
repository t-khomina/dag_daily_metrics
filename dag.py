# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Функция для CH
def ch_get_df(q):
    connection = {
        'host': '***',
        'password': '***',
        'user': '***',
        'database': '***'
    }
    result = pandahouse.read_clickhouse(q, connection=connection)
    return result


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 't-homina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 14),
}

# Интервал запуска DAG
schedule_interval = '0 9 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_homina():

    @task()
    def extract_feed():
        query = """SELECT 
                       user_id,
                       toDate(time) as event_date, 
                       MAX(gender) as gender,
                       multiIf(MAX(age) < 18, '<18',
                               MAX(age) >= 18 and MAX(age) < 25, '18-25',
                               MAX(age) >= 25 and MAX(age) < 35, '25-35',
                               MAX(age) >= 35 and MAX(age) < 45, '35-45',
                               MAX(age) >= 45 and MAX(age) < 55, '45-55',
                               '>=55') as age,
                       MAX(os) as os,
                       countIf(action = 'view') as views, 
                       countIf(action = 'like') as likes
                   FROM 
                        simulator_20220520.feed_actions 
                   WHERE 
                        toDate(time) = yesterday()
                   GROUP BY
                        user_id, event_date
                """
        df_feed = ch_get_df(q=query)
        return df_feed
    
    @task()
    def extract_mes():
        query = """SELECT user_id,
                          event_date,
                          gender,
                          age,
                          os,
                          messages_sent, 
                          users_sent,
                          messages_received,
                          users_received
                   FROM 
                       (SELECT user_id,
                               toDate(time) as event_date,
                               MAX(gender) as gender,
                               multiIf(MAX(age) < 18, '<18',
                                       MAX(age) >= 18 and MAX(age) < 25, '18-25',
                                       MAX(age) >= 25 and MAX(age) < 35, '25-35',
                                       MAX(age) >= 35 and MAX(age) < 45, '35-45',
                                       MAX(age) >= 45 and MAX(age) < 55, '45-55',
                                       '>=55') as age,
                               MAX(os) as os,
                               count(reciever_id) as messages_sent,
                               count(DISTINCT reciever_id) as users_sent        
                        FROM simulator_20220520.message_actions 
                        WHERE toDate(time) = yesterday()
                        GROUP BY user_id, event_date) t1
                   JOIN 
                       (SELECT reciever_id,
                               toDate(time) as event_date,
                               count(user_id) as messages_received,
                               count(DISTINCT user_id) as users_received     
                        FROM simulator_20220520.message_actions 
                        WHERE toDate(time) = yesterday()
                        GROUP BY reciever_id, event_date) t2    
                   ON t1.user_id = t2.reciever_id
                """
        df_mes = ch_get_df(q=query)
        return df_mes
    
    @task
    def transform_merge_tables(df_feed, df_mes):
        #df = df_feed.merge(df_mes, how='outer', on='user_id').fillna(0)
        df = df_feed.merge(df_mes, left_on = ['user_id','os','age','gender','event_date'], right_on =['user_id','os','age','gender','event_date'], how ='outer').fillna(0)
        return df

    @task
    def transform_gender(df):
        df_gender = df.groupby(['event_date', 'gender'])\
            .sum()\
            .reset_index()\
            .drop(columns = 'user_id')
        df_gender['metric'] = 'gender'
        df_gender.rename(columns = {'gender': 'metric_value'}, inplace=True)
        return df_gender

    @task
    def transform_age(df):
        df_age = df.groupby(['event_date', 'age'])\
            .sum()\
            .reset_index()\
            .drop(columns = ['user_id', 'gender'])
        df_age['metric'] = 'age'
        df_age.rename(columns = {'age': 'metric_value'}, inplace=True)
        return df_age
    
    @task
    def transform_os(df):
        df_os = df.groupby(['event_date', 'os'])\
            .sum()\
            .reset_index()\
            .drop(columns = ['user_id', 'gender'])
        df_os['metric'] = 'os'
        df_os.rename(columns = {'os': 'metric_value'}, inplace=True)
        return df_os
    
    @task
    def transform_concat_tables(df_gender, df_age, df_os):
        final_df = pd.concat([df_gender, df_age, df_os]).reset_index(drop=True)
        final_df = final_df[['event_date',
                             'metric',
                             'metric_value', 
                             'views', 
                             'likes', 
                             'messages_received',
                             'messages_sent',
                             'users_received', 
                             'users_sent']]        
        return final_df

    @task
    def load(final_df):
        connection_test = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': '656e2b0c9c',
            'user': 'student-rw',
            'database': 'test'
        }
        final_df = final_df.astype({'views': 'int64',
                                    'likes': 'int64',
                                    'messages_sent': 'int64',
                                    'users_sent': 'int64',
                                    'messages_received': 'int64',
                                    'users_received': 'int64'})
        query = '''CREATE TABLE IF NOT EXISTS test.thomina
                (event_date Date,
                 metric String,
                 metric_value String,
                 views Int64,
                 likes Int64,
                 messages_received Int64,
                 messages_sent Int64,
                 users_received Int64,
                 users_sent Int64
                ) ENGINE = Log()'''
        pandahouse.execute(query=query, connection=connection_test)
        pandahouse.to_clickhouse(df = final_df, table = 'thomina', index = False, connection = connection_test)

    df_feed = extract_feed()
    df_mes = extract_mes()
    df = transform_merge_tables(df_feed, df_mes)
    df_gender = transform_gender(df)
    df_age = transform_age(df)
    df_os = transform_os(df)
    final_df = transform_concat_tables(df_gender, df_age, df_os)
    load(final_df)

dag_homina = dag_homina()
