import pyodbc
from dbconfig import read_db_config


def db_conn():
    db_config = read_db_config()
    conn = None
    try:
        conn = pyodbc.connect(**db_config)
        cursor=conn.cursor()
        try:
            sql_command="""SELECT @@Version"""
            cursor.execute(sql_command)
            for row in cursor:
                print(f'row={row}')
        except Exception as e:
            print("Table not available",e)
    except Exception as e:
        print("error",e)
    return conn

    # finally:
    #     if conn is not None:
    #         conn.close()
    #         print('Connection closed.')

if __name__ == '__main__':
    print(db_conn())