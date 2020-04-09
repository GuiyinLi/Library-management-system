'''
    对于数据库链接对象来说，具有以下操作
        commit()            --事务提交
        rollback()          --事务回滚
        close()             --关闭一个数据库链接
        cursor()            --创建一个游标

    cur = conn.cursor()
    这样我们就创建了一个游标对象：cur
    在MySQL中，所有sql语句的执行都要在游标对象的参与下完成
    对于游标对象cur，具有以下具体操作：
        execute()           --执行一条sql语句
        executemany()       --执行多条sql语句
        close()             --游标关闭
        fetchone()          --从结果中取出一条记录
        fetchmany()         --从结果中取出多条记录
        fetchall()          --从结果中取出所有记录
        scroll()            --游标滚动
        update()            --更新数据
        delete()            --删除数据
'''
import logging

import pymysql

'''
Filename：指定路径的文件。这里使用了+__name__+是将log命名为当前py的文件名
Format：设置log的显示格式（即在文档中看到的格式）。分别是时间+当前文件名+log输出级别+输出的信息
Level：输出的log级别，优先级比设置的级别低的将不会被输出保存到log文档中
Filemode： log打开模式
a：代表每次运行程序都继续写log。即不覆盖之前保存的log信息。
w：代表每次运行程序都重新写log。即覆盖之前保存的log信息
'''
# 加入日志
logging.basicConfig(filename='d:/VS_Code_program/Python/Database_Test/log/'+__name__+'.log',
                    format='%(asctime)s %(levelname)-7s:%(message)s', level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %H:%M:%S')
# 获取logger实例
logger = logging.getLogger()

# MySQL数据库操作类


class DB_MySql():

    # 是否打印信息
    SHOW_Info = True

    # 构造方法，连接数据库，端口为3306，默认连接root用户下的数据库
    def __init__(self):
        # 数据库信息
        self.__conInfo = {'host': '', 'port': None, 'user': '',
                          'password': '', 'db': '', 'charset': ''}
        # 连接
        self.__conn = None
        # 游标
        self.__cursor = None
        # 缓存建立表格字段
        self.__table_head = tuple()

    # 连接数据库
    def connectDatabase(self, user, password, db, host='localhost', port=3306, charset='utf8'):
        # 每次进行连接时，重置变量信息
        self.__conInfo['host'] = host
        self.__conInfo['port'] = port
        self.__conInfo['user'] = user
        self.__conInfo['password'] = password
        self.__conInfo['db'] = db
        self.__conInfo['charset'] = charset
        self.__conn = None
        self.__cursor = None
        try:
            self.__conn = pymysql.connect(
                host=self.__conInfo['host'], port=self.__conInfo['port'], user=self.__conInfo['user'], passwd=self.__conInfo['password'], db=self.__conInfo['db'], charset=self.__conInfo['charset'])
            self.__cursor = self.__conn.cursor()
            self.__printInfo("Database connection successful", 'T')
            return True
        except pymysql.Error as e:
            error = 'DataBase connection failed ' + str(e)
            self.__printInfo(error, 'E')
            return False

    # 析构方法，关闭数据库
    def __del__(self):
        if self. __conn and self.__cursor:
            self.__cursor.close()
            try:
                self.__conn.ping(False)
                self.__conn.commit()
                self.__conn.close()
            except pymysql.Error:
                pass
        print('EXIT')

    # 关闭数据库
    def close(self):
        if self. __conn and self.__cursor:
            self.__cursor.close()
            try:
                self.__conn.ping(False)
                self.__conn.commit()
                self.__conn.close()
                self.__printInfo('Database connection closed successful', 'T')
                return True
            except pymysql.Error as e:
                error = 'Database connection closed Failed : '+str(e.args)
                self.__printInfo(error, 'E')
                return False
        else:
            self.__printInfo('Database connection is not exist', 'E')
            return False

    # 打印信息
    def __printInfo(self, message, infoType='T'):
        if self.SHOW_Info:
            information1 = self.__conInfo['user']+'~' + \
                self.__conInfo['db']+'-'+infoType+': '+message
            print(information1)
        information2 = self.__conInfo['user']+'~' + \
            self.__conInfo['db']+'@'+message
        if infoType in ('T', 'R'):
            logger.info(information2)
        elif infoType == 'W':
            logger.warning(information2)
        elif infoType == 'E':
            logger.error(information2)
        else:
            logger.warning("illegal information")

    # 获取游标对象
    def getCursor(self):
        if self.__cursor is None:
            self.__printInfo(
                "The cursor is not exist,database may not connect", 'W')
        return self.__cursor

    # 获取游标所有信息 返回列表，列表的每个元素为元组
    def getCursorInfo(self):
        '''
            if the result of fetch is not empty,return list(tuple1,tuple2,...,tuplen)\n
            if the result of fetch is empty,return list(description)\n
            if not fetch or cursor is not exist,return None\n
        '''
        cursorInfo = None
        if self.__cursor:
            try:
                result = self.__cursor.fetchall()  # fetch后，内容置空
                if self.__cursor.description:
                    description = tuple([desc[0]
                                         for desc in self.__cursor.description]),
                cursorInfo = list(description+result)
                if len(result) > 0:
                    for r in result:
                        rs = str(r)
                        self.__printInfo(rs, 'R')
                else:
                    self.__printInfo("No results", 'R')
            except pymysql.Error as e:
                error = 'Cursor fetch failed : '+str(e.args)
                self.__printInfo(error, 'E')
        else:
            self.__printInfo(
                "The cursor is not exist,database may not connect", 'W')
        return cursorInfo

    # 用来处理SQL语句中的字符串字典值，在两侧加双引号
    def __process(self, s):
        if isinstance(s, str):
            return '"'+s+'"'
        return str(s)

    # 该方法用来执行SQL语句 ，若链接断开则自动重连，执行之后自动返回游标对象
    def executeSql(self, sql):
        if self.__conn and self.__cursor:
            # check the connection
            try:
                self.__conn.ping()
                self.__cursor = self.__conn.cursor()
            except pymysql.Error as e:
                self.__printInfo(
                    "The database cannot connect", 'E')
            else:
                # execute SQL
                try:
                    self.__cursor.execute(sql)
                    self.__conn.commit()
                    self.__printInfo(
                        'SQL->[{}]'.format(sql)+' execute successful', 'T')
                except pymysql.Error as e:
                    # 发生错误时回滚
                    self.__conn.rollback()
                    error = 'SQL->[{}]'.format(sql) + \
                        ' execute failed : ' + str(e.args)
                    self.__printInfo(error, 'E')
        else:
            self.__printInfo(
                "Database connection is not exist,please check the database", 'E')
        return self.__cursor

    # 创建数据库表
    def create_table(self, table_name, table_head):
        if table_name != '':
            sql = 'CREATE TABLE if not exists '+table_name+' ('+table_head+')'
            table_head = table_head.replace(',', ' ')
            self.__table_head = table_head.split()
            self.__table_head = self.__table_head[::2]
            if self.SHOW_Info:
                print('执行sql:[{}]'.format(sql))
            self.executeSql(sql)
            print('创建数据库表[{}]成功!'.format(table_name))
        else:
            print('the [{}] is empty or equal None!'.format(table_name))
        return table_name

    # 如果表存在,则删除表
    def drop_table(self, table_name):
        sql = 'DROP TABLE IF EXISTS ' + table_name
        self.executeSql(sql)
        print('删除数据库表[{}]成功!'.format(table_name))

    # 插入记录
    def insert(self, table_name, data):
        sql = 'INSERT INTO '+table_name+' VALUES('
        if data:
            for item in data[:-1]:
                sql = sql+self.__process(item)+','
            sql = sql + self.__process(data[-1])+')'
            self.executeSql(sql)
        else:
            print('nothing to insert')

    # 更新记录
    def update(self, table_name, condition, toData):
        if toData:
            sql = 'UPDATE '+table_name+' SET '
            i = 0
            for item in toData[:-1]:
                sql = sql+str(self.__table_head[i]) + \
                    '='+self.__process(item)+','
                i += 1
            sql = sql+str(self.__table_head[-1]) + \
                '='+self.__process(toData[-1])+' WHERE '
            for key, value in condition.items():
                sql = sql+str(key)+'='+self.__process(value)+' and '
            sql = sql[:len(sql)-5]
            self.executeSql(sql)
        else:
            print('nothing to update')

    # 删除记录
    def delete(self, table_name, condition):
        if condition is not None:
            sql = 'DELETE FROM '+table_name+' WHERE '
            for key, value in condition.items():
                sql = sql+str(key)+'='+self.__process(value)+' and '
            sql = sql[:len(sql)-5]
            self.executeSql(sql)
        else:
            print('nothing to delete')

    # 查询所有记录
    def fetchall(self, table_name):
        sql = 'select * from '+table_name
        cur = self.executeSql(sql)
        r = cur.fetchall()
        return list(r)

    # 查询多条记录 条件不能为空，返回列表
    def select(self, table_name, condition, fields='*'):
        queryResult = None
        if len(condition) > 0:
            sql = 'select '+fields+' from '+table_name+' where '
            for key, value in condition.items():
                sql = sql+str(key)+'='+self.__process(value)+' and '
            sql = sql[:len(sql)-5]
            # 元组为空!=None,not二者均可检测
            self.executeSql(sql)
            queryResult = self.getCursorInfo()
        else:
            self.__printInfo('The query condition is empty', 'W')
        return queryResult
