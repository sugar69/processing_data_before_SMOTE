import sqlite3

conn_1 = sqlite3.connect('shishir-a412ed_docker_hash_tag_date.db')
cursor_1 = conn_1.cursor()
conn_2 = sqlite3.connect('shishir-a412ed_docker.db')
cursor_2 = conn_2.cursor()

def slice_db():
    timestamp_target = ""
    timestamp_previous = ""
    # 行数を獲得する
    try:
        cursor_1.execute("SELECT COUNT(tag) FROM 'shishira412ed_docker_hash_tag_date'")
        column_num_tuple = cursor_1.fetchone()
        # print(column_num_tuple)
        column_num = column_num_tuple[0]

    except sqlite3.Error as e:
        print('sqlite3.Error occurred:', e.args[0])

    # 1つ目のリリース以前のものにフラグをつける
    try:
        # cursor_1.execute("SELECT author_date_unix_timestamp FROM shishir-a412ed_docker_hash_tag_date WHERE id = '%s'" %'1')
        cursor_1.execute("SELECT author_date_unix_timestamp FROM 'shishira412ed_docker_hash_tag_date'")
        timestamp_target_tuple = cursor_1.fetchone()
        # # print(timestamp_target_tuple)
        # timestamp_target_tuple = cursor_1.fetchall()
        # print(timestamp_target_tuple)
        timestamp_target = timestamp_target_tuple[0]
        cursor_2.execute("UPDATE 'shishir-a412ed_docker' SET author_date_flag = '1' WHERE author_date_unix_timestamp < '%s'"
                         % timestamp_target)

    except sqlite3.Error as e:
        print('sqlite3.Error occurred:', e.args[0])

    timestamp_previous = timestamp_target


    # dbをリリースごとにフラグをつける。author_date_unix_timestampを用いる
    for loop_num in range(1, column_num):
        try:
            # cursor_1.execute("SELECT author_date_unix_timestamp FROM shishir-a412ed_docker_hash_tag_date WHERE id = '%s'" % str(loop_num+1))
            timestamp_target_tuple = cursor_1.fetchone()
            print(timestamp_target_tuple, loop_num+1)
            timestamp_target = timestamp_target_tuple[0]
#            insert_data = [{'num': loop_num+1, 'timestamp': timestamp_target}]
            cursor_2.execute("UPDATE 'shishir-a412ed_docker' SET author_date_flag = '%d' WHERE '%s' <= author_date_unix_timestamp AND "
                             "author_date_unix_timestamp < '%s'" % (loop_num+1, timestamp_previous, timestamp_target))
            print(loop_num+1)

        except sqlite3.Error as e:
            print('sqlite3.Error occurred:', e.args[0])

        timestamp_previous = timestamp_target

        # 最後のリリース以降のものにフラグをつける
        try:
            # cursor_1.execute("SELECT author_date_unix_timestamp FROM shishir-a412ed_docker_hash_tag_date WHERE id = '%s'" %'1')
            # cursor_1.execute("SELECT author_date_unix_timestamp FROM shishir-a412ed_docker_hash_tag_date")
            # timestamp_target_tuple = cursor_1.fetchone()
            # # # print(timestamp_target_tuple)
            # # timestamp_target_tuple = cursor_1.fetchall()
            # # print(timestamp_target_tuple)
            # timestamp_target = timestamp_target_tuple[0]
            cursor_2.execute("UPDATE 'shishir-a412ed_docker' SET author_date_flag = '%d' WHERE '%s' <= author_date_unix_timestamp"
                             % (column_num+1, timestamp_previous))

        except sqlite3.Error as e:
            print('sqlite3.Error occurred:', e.args[0])


slice_db()

conn_1.commit()
conn_2.commit()

# 接続を閉じる
conn_1.close()
conn_2.close()
