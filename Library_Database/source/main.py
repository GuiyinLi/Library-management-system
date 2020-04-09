'''
@Author: Ken Kaneki
@Date: 2020-04-04 18:13:22
@LastEditTime: 2020-04-09 18:50:45
@Description: README
@FilePath: \Library_system\Database_Test\source\main.py
'''

from DataBase_MySql import DB_MySql

db = DB_MySql()
db.connectDatabase(user='root', password='170707109', db='university schema')
result = db.select('takes', {'ID': '45678', 'course_id': 'CS-101'})
# db.insert('takes', ['00128', 'CS-190', 2, 'Fall', '2009', 'A'])
db.close()
