import sqlite3
import pandas as pd
import re


def strip_tags(value):
    return re.sub(r'<[^>]*>', '', str(value))


def create_database():
    con = sqlite3.connect('works.sqlite')
    cur = con.cursor()
    cur.execute('PRAGMA foreign_keys = true')
    con.commit()
    return con, cur


def make_works_table(cur):
    cur.execute('DROP TABLE IF EXISTS works')
    cur.execute('CREATE TABLE works ('
                'ID INTEGER PRIMARY KEY AUTOINCREMENT,'
                'salary INTEGER,'
                'educationType TEXT,'
                'jobTitle TEXT,'
                'qualification TEXT,'
                'gender TEXT,'
                'dateModify TEXT,'
                'skills TEXT,'
                'otherInfo TEXT)')


def clean_tags(con):
    df = pd.read_csv("works.csv")
    df['skills'] = df['skills'].apply(strip_tags)
    df['otherInfo'] = df['otherInfo'].apply(strip_tags)
    df.to_sql("works", con, if_exists='append', index=False)
    con.commit()


def make_genders_and_education(cur, con):
    cur.execute('DROP TABLE IF EXISTS genders')
    cur.execute('CREATE TABLE genders(genderName TEXT PRIMARY KEY )')
    cur.execute('INSERT INTO genders SELECT DISTINCT gender FROM works WHERE gender IS NOT NULL')
    cur.execute('DROP TABLE IF EXISTS educations')
    cur.execute('CREATE TABLE educations(educationType TEXT PRIMARY KEY )')
    cur.execute('INSERT INTO educations SELECT DISTINCT educationType FROM works WHERE works.educationType IS NOT NULL')
    con.commit()


def update_works(cur, con):
    cur.execute('CREATE TABLE new_works ('
                'ID INTEGER PRIMARY KEY AUTOINCREMENT,'
                'salary INTEGER,'
                'educationType TEXT REFERENCES educations(educationType) ON DELETE CASCADE ON UPDATE CASCADE,'
                'jobTitle TEXT,'
                'qualification TEXT,'
                'gender TEXT REFERENCES genders(genderName) ON DELETE CASCADE ON UPDATE CASCADE,'
                'dateModify TEXT,'
                'skills TEXT,'
                'otherInfo TEXT)')
    cur.execute('INSERT INTO new_works SELECT * FROM works')
    cur.execute('DROP TABLE works')
    cur.execute('ALTER TABLE new_works RENAME TO works')
    con.commit()


if __name__ == '__main__':
    connection = create_database()[0]
    cursor = create_database()[1]
    make_works_table(cursor)
    clean_tags(connection)
    make_genders_and_education(cursor, connection)
    update_works(cursor, connection)

