import requests
from bs4 import BeautifulSoup
import sqlite3


def db_init(db_file='maklai.db'):
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS blogs(
       id INT PRIMARY KEY,
       title TEXT,
       create_date TEXT,
       text TEXT,
       author TEXT
       );
    """)

    cur.execute("""CREATE TABLE IF NOT EXISTS releases(
       id INT PRIMARY KEY,
       url TEXT,
       title TEXT,
       create_date TEXT,
        text TEXT
       );
    """)

    cur.execute("""CREATE TABLE IF NOT EXISTS files(
       id INT PRIMARY KEY,
       file_url TEXT,
       version TEXT,
       os TEXT,
        description TEXT,
        md5 TEXT,
        file_size INTEGER,
        gpg TEXT,
        ask_url Text,
        release_id INTEGER
       );
    """)

    cur.execute("""CREATE TABLE IF NOT EXISTS peps(
       id INT PRIMARY KEY,
       url TEXT
       );
    """)

    cur.execute("""CREATE TABLE IF NOT EXISTS pep_release(
       id INT PRIMARY KEY,
       pep_id INTEGER,
       release_id INTEGER
       );
    """)

    cur.execute("""CREATE TABLE IF NOT EXISTS blog_release(
       id INT PRIMARY KEY,
       blog_id INTEGER,
       release_id INTEGER
       );
    """)

    conn.commit()
    return conn, cur


conn, cur = db_init()
py_blogs = requests.get('https://blog.python.org/')
soup = BeautifulSoup(py_blogs.text, 'html.parser')
years = [i.contents[1].contents[3].get('href') for i in soup.find(id="BlogArchive1_ArchiveList").contents[1::2]]
blog_id = 0

for year in years:
    print(year)
    resp = requests.get(year)
    soup = BeautifulSoup(resp.text, 'html.parser')
    for blog in soup.find_all(class_='date-outer'):
        try:
            title = blog.find(class_='post-title entry-title').text.strip()
        except:
            title = blog.find(class_='post hentry').find('span').text.strip()
        date = blog.find(class_='date-header').text.strip()
        text = ' '.join([i.text for i in blog.find(class_='post hentry').find_all(['p', 'strong', 'em', 'div'])][:-1])
        print(date, title, text)
        author = blog.find(class_='fn').text
        releases = [i.get('href') for i in blog.find(
            class_='post-body entry-content').find_all('a') if 'https://www.python.org/downloads/release' in i.text]
        if releases == []:
            releases = [i.text for i in blog.find(
                class_='post-body entry-content').find_all('u') if 'https://www.python.org/downloads/release' in i.text]

        cur.execute("INSERT INTO blogs VALUES(?, ?, ?, ?, ?);", (blog_id, title, date, text, author))
        conn.commit()
        for release in releases:
            print(release)
            try:
                cur.execute('SELECT * FROM releases WHERE url=?;', (release,))
                result = cur.fetchone()
                if not result:
                    resp = requests.get(release.strip())
                    rel = BeautifulSoup(resp.text, 'html.parser')
                    release_title = rel.find(class_='page-title').text
                    release_date = rel.find(class_='text').find('p').text.split(':')[1]
                    release_text = ' '.join([i.text for i in rel.find(class_='text').find_all(('p',
                                                                                               'strong',
                                                                                               'h2',
                                                                                               'ul',
                                                                                               'h1',
                                                                                               'a'))])
                    peps = [i.get('href') for i in rel.find_all('a') if 'peps' in i.get('href')]
                    files = [[i.find_all('td')[0].find('a').get('href')] + [j.text for j in i.find_all('td')
                                                                            ] + [
                                 i.find_all('td')[-1].find('a').get('href')
                                 ] for i in rel.find('tbody').find_all('tr')]
                    cur.execute("SELECT id FROM releases;")
                    results = cur.fetchall()
                    if results == []:
                        release_id = 0
                    else:
                        release_id = max([i[0] for i in results]) + 1
                    cur.execute('INSERT INTO releases(id,url,title,create_date,text) VALUES (?,?,?,?,?);',
                                (release_id, release, release_title, release_date, release_text))
                    conn.commit()
                    for file in files:
                        cur.execute("SELECT id FROM files;")
                        results = cur.fetchall()
                        if results == []:
                            file_id = 0
                        else:
                            file_id = max([i[0] for i in results]) + 1

                        cur.execute(
                            'INSERT INTO files(id,file_url,version,os,description,md5,file_size,gpg,ask_url,release_id'
                            ') VALUES (?,?,?,?,?,?,?,?,?,?);', [file_id] + file + [release_id])
                        conn.commit()
                    for pep in peps:
                        cur.execute("SELECT id FROM peps WHERE url=?;", (pep,))
                        result = cur.fetchone()
                        if not result:
                            cur.execute("SELECT id FROM peps;")
                            results = cur.fetchall()
                            if results == []:
                                pep_id = 0
                            else:
                                pep_id = max([i[0] for i in results]) + 1
                            cur.execute('INSERT INTO peps(id,url) VALUES (?,?);', (pep_id, pep,))
                            conn.commit()
                        cur.execute('INSERT INTO pep_release(pep_id,release_id) VALUES (?,?);', (pep_id, release_id,))
                        conn.commit()
                else:
                    release_id = result[0]
                cur.execute('SELECT * FROM blog_release WHERE (release_id=?)and(blog_id=?);', (release_id, blog_id))
                result = cur.fetchone()

                if not result:
                    cur.execute("INSERT INTO blog_release(release_id, blog_id) VALUES(?, ?);",
                                (release_id, blog_id))
                    conn.commit()
            except Exception as e:
                print(e)

        blog_id += 1
