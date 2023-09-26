# -*- coding:utf-8 -*-
import datetime
import time
import concurrent.futures
import pymysql
import sys
def run(table_name, date_column,some_day,mclient):
      day = datetime.datetime.today() - datetime.timedelta(int(30))
      day = day.strftime('%Y-%m-%d') + " 00:00:00"
      time_stamp = int(time.mktime(time.strptime(day, '%Y-%m-%d %H:%M:%S'))) * 1000
      print('%s 数据删除开始'%(day))
      start_time1 = time.time()
      db_cursor = client_conn.cursor()
      sql_query  = f'select id from {table_name} where {date_column}  <= {time_stamp} limit 5000 '
      print(sql_query)
      db_cursor.execute(sql_query)
      result = db_cursor.fetchall()
      while len(result)!=0:
        start_time = time.time()
        try:
            for v_data in result:
                sql = f'delete from '+table_name+ ' where id =%s'%v_data[0]
                # print(sql)
                db_cursor.execute(sql)
                mclient.commit()
        except Exception as e:
            mclient.rollback()
            print(e)
        end_time = time.time()
        elapsed_time = end_time - start_time

        print("耗时5000条：%s" % elapsed_time)

        db_cursor.execute(sql_query)
        result = db_cursor.fetchall()
      db_cursor.close()
      end_time1 = time.time()
      elapsed_time1 = end_time1 - start_time1
      print("总的耗时：%s" % elapsed_time1)
      print(f"删除数据 {table_name}，小于时间：{day}")

#
# def get_all_day(some_day):
#     t = datetime.date.today()
#     ret=[]
#     i = 1
#     while i <= some_day:
#         t1 = datetime.datetime.today() - datetime.timedelta(i)
#         t1 = t1.strftime('%Y-%m-%d')
#         ret.append(t1)
#         i=i+1
#
#     return  list(reversed(ret))


def getConn():
   return pymysql.connect(host = '127.0.0.1', user = 'test', passwd = 'test', db= 'test')

if __name__ == '__main__':
    parame=sys.argv[1]
    client_conn=getConn()
  
    for  v in  parame.split("\n"):
        if len(v.strip())>0:
            table_name=v.strip().split(",")[0]
            date_column=v.strip().split(",")[1]
            some_day=v.strip().split(",")[2]
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                executor.submit(run, table_name, date_column,some_day,client_conn)
