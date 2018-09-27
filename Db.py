# import pymysql
import MySQLdb
import sys
import io
from urllib.request import urlopen
sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8')

'''
数据库操作类
'''
class MysqldbHelper(object):
	# 数据库连接
	db = ''
	# 操作游标
	cursor = ''

	'''
	@param Dict database 数据库配置信息
	格式如 {
	    'DB': 'xxx',
	    'HOST': 'xxx',
	    'PORT': 3306,
	    'USER': 'xxx',
	    'PASSWORD': 'xxx',
	    'CHARSET': 'utf8'
	}
	'''
	def __init__(self, database):
		try:
			# 连接数据库
			# self.db = pymysql.connect(database['HOST'], database['USER'], database['PASSWORD'], database['DB'])
			self.db = MySQLdb.connect(host = database['HOST'], user = database['USER'], passwd = database['PASSWORD'], db = database['DB'], port = database['PORT'], charset = database['CHARSET'])
			self.cursor = self.db.cursor()
		except Exception as e:
			print('MySQLdb Error:%s' % e)

	'''
	执行sql语句
	@param Str sql SQL语句。
	'''
	def exec(self, sql):
		try:
			if type(sql) is list:
				count = 0
				for row in sql:
					count += self.cursor.execute(row)
			else:
				count = self.cursor.execute(sql)
			# 提交到数据库执行
			self.db.commit()
			return count
		except Exception as e:
			# 发生错误时回滚
			self.db.rollback()
			print('MySQLdb Error:%s' % e)

	'''
	sql语句查询多条记录
	'''
	def findAllSql(self, sql):
		try:
			result = self.execSelectAll(sql)

			return result
		except Exception as e:
			print('MySQLdb Error:%s' % e)

	'''
	sql语句查询单条记录
	'''
	def findOneSql(self, sql):
		try:
			result = self.execSelectOne(sql)

			return result
		except Exception as e:
			print('MySQLdb Error:%s' % e)

	'''
    获取多条记录
    @param Str fields 字段名。  格式如 'field1,field2,...'
    @param Str table_name 表名称。
    @param Str conditions 要插入的数据。	格式如 'AND cond1=value1 ...'
    '''
	def findAll(self, fields, table_name, conditions=''):
		try:
			if len(fields) == 0:
				print('fields is empty!')
				exit()

			if len(table_name) == 0:
				print('table name is empty!')
				exit()

			sql = "SELECT " + fields + " FROM " + table_name + " WHERE 1=1 " + conditions
			result = self.execSelectAll(sql)

			return result
		except Exception as e:
			print('MySQLdb Error:%s' % e)

	'''
    获取单条记录
    @param Str fields 字段名。  格式如 'field1,field2,...'
    @param Str table_name 表名称。
    @param Str conditions 要插入的数据。	格式如 'AND cond1=value1 ...'
    '''
	def findOneSql(self, fields, table_name, conditions=''):
		try:
			if len(fields) == 0:
				print('fields is empty!')
				exit()

			if len(table_name) == 0:
				print('table name is empty!')
				exit()

			sql = "SELECT " + fields + " FROM " + table_name + " WHERE 1=1 " + conditions + " LIMIT 1"
			result = self.execSelectOne(sql)

			return result
		except Exception as e:
			print('MySQLdb Error:%s' % e)


	'''
    执行sql语句获取多条记录
    '''
	def execSelectAll(self, sql):
		try:
			self.cursor.execute(sql)
			# 获取字段名
			index = self.cursor.description
			fields_count = len(index)
			result = []
			records = self.cursor.fetchall()

			for res in records:
				row = {}
				# 加上字段名
				if fields_count > 1:
					for i in range(fields_count-1):
						row[index[i][0]] = res[i]
				else:
					row[index[0][0]] = res[0]

				result.append(row)

			return result

		except Exception as e:
			print('MySQLdb Error:%s' % e)

	'''
    执行sql获取单条记录
    '''
	def execSelectOne(self, sql):
		try:
			self.cursor.execute(sql)
			#获取字段名
			index = self.cursor.description
			fields_count = len(index)
			records = self.cursor.fetchall()

			for res in records:
				result = {}
				# 加上字段名
				if fields_count > 1:
					for i in range(fields_count-1):
						result[index[i][0]] = res[i]
				else:
					result[index[0][0]] = res[0]

			return result
		except Exception as e:
			print('MySQLdb Error:%s' % e)

	'''
    插入数据
    @param List fields 字段名。  格式如 [field1, field2, ...]
    @param Str table_name 表名称。
    @param List data 要插入的数据。	格式如 [{field1: value1, field2: value2, ...}]
    '''
	def insert(self, fields, table_name, data):
		try:
			if len(fields) == 0:
				print('fields is empty!')
				exit()

			if len(data) == 0:
				print('data is empty!')
				exit()

			if len(table_name) == 0:
				print('table name is empty!')
				exit()

			field_str = ''

			for field in fields:
				field_str += field + ','

			field_str = field_str[:-1]

			sql = 'INSERT INTO ' + table_name + ' (' + field_str + ') VALUES '

			for row in data:
				sql += '('
				for index in row:
					sql += '\'' + str(row[index]) + '\',' if type(row[index]) is str else str(row[index]) + ','
				sql = sql[:-1]
				sql += '),'

			sql = sql[:-1]

			count = self.cursor.execute(sql)
			self.db.commit()
			
			return count

		except Exception as e:
			self.db.rollback()
			print('MySQLdb Error:%s' % e)

	'''
    更新记录
    @param Str table_name 表名称。
    @param List data 要插入的数据。	格式如 [{field1: value1, field2: value2, ...}]
    @param Str conditions 过滤条件。	格式如 'AND cond1=value1 ...'
    '''
	def update(self, table_name, data, conditions):
		try:
			if len(data) == 0:
				print('data is empty!')
				exit()

			if len(table_name) == 0:
				print('table name is empty!')
				exit()

			if len(conditions) == 0:
				print('conditions is empty!')
				exit()

			sql = 'UPDATE ' + table_name + ' SET '
			for row in data:
				for index in row:
					sql += index + '=\'' + str(row[index]) + '\',' if type(row[index]) is str else index + '=' + str(row[index]) + ','
				sql = sql[:-1]

			sql += ' where 1=1 ' + conditions

			count = self.cursor.execute(sql)
			self.db.commit()

			return count
		except Exception as e:
			self.db.rollback()
			print('MySQLdb Error:%s' % e)

	'''
    删除数据
    @param Str table_name 表名称。
    @param Str conditions 过滤条件。	格式如 'AND cond1=value1 ...'
    '''
	def delete(self, table_name, conditions):
		try:
			if len(table_name) == 0:
				print('table name is empty!')
				exit()

			if len(conditions) == 0:
				print('conditions is empty!')
				exit()

			sql = 'DELETE FROM ' + table_name + ' WHERE 1=1 ' + conditions

			count = self.cursor.execute(sql)
			self.db.commit()

			return count

		except Exception as e:
			self.db.rollback()
			print('MySQLdb Error:%s' % e)

	def __del__(self):
		try:
			# 关闭数据库
			self.db = self.db.close()
		except Exception as e:
			print('MySQLdb Error:%s' % e)
