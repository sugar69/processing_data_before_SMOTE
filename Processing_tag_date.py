import subprocess
import sqlite3

# conn = sqlite3.connect('hadoop_hash_tag_date.db')
conn = sqlite3.connect('cyphar_docker_hash_tag_date.db')
cursor = conn.cursor()

def make_dictionary_releasedate():
    git_tag_dict = {}
    git_hash_dict = {}
    git_authordate_dict = {}
    git_authordate_unixtimestamp_dict = {}
    result_dict = {}

    # 通った(test block)
#    proc_ls = subprocess.Popen(['ls'], cwd='./hadoop/', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#    ls_byte = proc_ls.communicate()
#    print(type(ls_byte[0]))
#    ls_str = ls_byte[0].decode('utf-8')
    # print(test_str)
#    print(ls_str.split('\n'))
    # ここまで

    # git tagコマンドによってタグを取得。tag_strにリスト構造で保存
    proc_tag = subprocess.Popen(['git', 'tag'], cwd='./cyphar_docker/', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    tag_byte = proc_tag.communicate()
    tag_str = tag_byte[0].decode('utf-8')
    for tag_num in range(0, len(tag_str.split('\n'))-1):
        git_tag_dict[tag_num] = tag_str.split('\n')[tag_num]
    # ↓testcode
    print(tag_str.split('\n'))
#    print(tag_str.split('\n')[0])  # これでタグの要素１つだけ取り出せる
#    for test_num in range(0, len(git_tag_dict)):
#        print(git_tag_dict[test_num])
#        print("---split---%d", test_num)

    # git showコマンドによってgit hashを取得
    for tag_num in range(0, len(git_tag_dict)):
        proc_hash = subprocess.Popen(['git', 'show', '-s', '--format=%H', git_tag_dict[tag_num]], cwd='./cyphar_docker/',
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        hash_byte = proc_hash.communicate()
        hash_str = hash_byte[0].decode('utf-8')
        # warning対策
        if 'warning: you may want to set your diff.renameLimit variable' in hash_str.split('\n')[-2]:
            git_hash_dict[tag_num] = hash_str.split('\n')[-4]
        else:
            git_hash_dict[tag_num] = hash_str.split('\n')[-2]
        # ↓test print
#        print(git_hash_dict[tag_num])
#        print("---split---")

#    print(len(git_tag_dict))
    # git showコマンドによってAuther_dateを取得
    for tag_num in range(0, len(git_tag_dict)):
        proc_adate = subprocess.Popen(['git', 'show', '-s', '--format=%ad', git_tag_dict[tag_num]], cwd='./cyphar_docker/',
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        adate_byte = proc_adate.communicate()
        adate_str = adate_byte[0].decode('utf-8')
        # warning対策
        if 'warning: you may want to set your diff.renameLimit variable' in adate_str.split('\n')[-2]:
            git_authordate_dict[tag_num] = adate_str.split('\n')[-4]
        else:
            git_authordate_dict[tag_num] = adate_str.split('\n')[-2]
#        print(adate_str.split('\n'))
        # ↓test print
#        print(git_authordate_dict[tag_num])
#        print("---split---%d", tag_num)
#    print(type(git_authordate_dict))

    # git showコマンドによってAuther_dateを取得
    for tag_num in range(0, len(git_tag_dict)):
        proc_unixtimestamp = subprocess.Popen(['git', 'show', '-s', '--format=%at', git_tag_dict[tag_num]], cwd='./cyphar_docker/',
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        unixtimestamp_byte = proc_unixtimestamp.communicate()
        unixtimestamp_str = unixtimestamp_byte[0].decode('utf-8')
        # warning対策
        if 'warning: you may want to set your diff.renameLimit variable' in unixtimestamp_str.split('\n')[-2]:
            git_authordate_unixtimestamp_dict[tag_num] = unixtimestamp_str.split('\n')[-4]
        else:
            git_authordate_unixtimestamp_dict[tag_num] = unixtimestamp_str.split('\n')[-2]


    create_db(git_tag_dict, git_hash_dict, git_authordate_dict, git_authordate_unixtimestamp_dict)


    return result_dict

def create_db(tag_dict, hash_dict, authordate_dict, unixtimestamp_dict):
    try:
        cursor.execute("DROP TABLE IF EXISTS shishira412ed_docker_hash_tag_date")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS shishira412ed_docker_hash_tag_date (hash TEXT, tag TEXT PRIMARY KEY, author_date TEXT,"
            " author_date_unix_timestamp TEXT)")

        key_list = sorted(list(hash_dict.keys()))
#        print(len(hash_dict))
#        print(len(set([hash_dict[key] for key in key_list])))
        data = [{'hash': hash_dict[key], 'tag': tag_dict[key], 'author_date': authordate_dict[key],
                 'author_date_unix_timestamp': unixtimestamp_dict[key]} for key in key_list]
        cursor.executemany("INSERT INTO shishira412ed_docker_hash_tag_date VALUES (:hash, :tag, :author_date,"
                           " :author_date_unix_timestamp)", data)

    except sqlite3.Error as e:
        print('sqlite3.Error occurred:', e.args[0])



make_dictionary_releasedate()

conn.commit()

# 接続を閉じる
conn.close()