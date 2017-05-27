#! /usr/bin/env python3
# _*_ coding: utf-8 _*_
import asyncio, logging
import aiomysql

__author__ = "JS"

def log(sql, args=()):
	logging.info('SQL: %s' % sql)

async def create_pool(loop, **kw):
	logging.info('Create database connection pool....')
	global __pool

	__pool = await aiomysql.create_pool(
		host 	   = kw.get('host', '127.0.0.1'),
		port 	   = kw.get('port', 3306),
		user 	   = kw['user'],
		password   = kw['password'],
		db         = kw['database'],
		charset    = kw.get('charset', 'utf8'),
		autocommit = kw.get('autocommit', True),
		maxsize    = kw.get('maxsize', 10),
		minsize    = kw.get('minsize', 1),
		loop       = loop
	)


	
# async def destory_pool(): #销毁连接池
# 	global __pool
# 	if __pool is not None:
# 		__pool.close()
# 		await  __pool.wait_closed()

async def select(sql, args, size=None):
	log(sql, args)
	global __pool
	with (await __pool) as conn:
		cur = await conn.cursor(aiomysql.DictCursor)
		await cur.execute(sql.replace('?', '%s'), args or ())
		if size:
			rs = await cur.fetchmany(size)
		else:
			rs = await cur.fetchall()
		await cur.close()
		logging.info('rows returned: %s' % len(rs))
		return rs
async def execute(sql, args):
	log(sql)
	with (await __pool) as conn:
		try:
			cur=await conn.cursor()
			await cur.execute(sql.replace('?', '%s'), args)
			affected = cur.rowcount
			await cur.close()
		except BaseException as e:
			raise
		return affected

def create_args_string(num):
	L = []
	for n in range(num):
		L.append('?')
	return ','.join(L)

class Field(object):
	def __init__(self, name, column_type, primary_key, default):
		self.name        = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default     = default
	def __str__(self):
		return '<%s, %s: %s>' % (self.__class__.__name__, self.column_type, self.name)
class StringField(Field):
	"""docstring for StringField"""
	def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
		super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):
	"""docstring for BooleanField"""
	def __init__(self, name=None, default=False):
		super().__init__(name, 'boolean', False, default)

class IntegerField(Field):
	"""docstring for IntegerField"""
	def __init__(self, name=None, primary_key=False, default=0):
		super().__init__(name, 'bigint', primary_key, default)
		
class FloatField(Field):
	"""docstring for FloatField"""
	def __init__(self, name=None, primary_key=False, default=0.0):
		super().__init__(name, 'real', primary_key, default)

class TextField(Field):
	"""docstring for TextField"""
	def __init__(self, name=None, default=None):
		super().__init__(name, 'text', False, default)
		
		
		
class ModelMetaclass(type):
		"""docstring for MoudelMetaclass"""
		def __new__(cls, name, bases, attrs):
			#排除Model本身
			if name == 'Model':
				return type.__new__(cls, name, bases, attrs)
			#获取table名称
			tableName = attrs.get('__table__', None) or name
			logging.info('found model: %s (table: %s)' % (name, tableName))
			#获取所有Field和主键名
			mappings   = dict()
			fields     = []
			primaryKey = None
			for k, v in attrs.items():
				if isinstance(v, Field):
					logging.info('found mapping: %s ==> %s' % (k, v))
					mappings[k] = v
					if v.primary_key:
						#找到主键
						if primaryKey:
							raise RuntimeError('Duplicate primary key for field: %s' % k)
						primaryKey = k
					else:
						fields.append(k)
			if not primaryKey:
				raise RuntimeError('Primary key not found.')
			for k in mappings.keys():
				attrs.pop(k)
			escaped_fields = list(map(lambda f: '`%s`' % f, fields))
			attrs['__mappings__']    = mappings # 保存属性和列的映射关系
			attrs['__table__']       = tableName
			attrs['__primary_key__'] = primaryKey # 主键属性名
			attrs['__fields__']      = fields # 除主键外的属性名
			# 构造默认的SELECT INSERT UPDATE DETELE语句
			attrs['__select__']      = 'SELECT `%s`, %s FROM `%s`' % (primaryKey, ','.join(escaped_fields), tableName)
			attrs['__insert__']      = 'INSERT INTO `%s` (%s, `%s`) VALUES (%s)' % (tableName, ','.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
			attrs['__update__']      = 'UPDATE `%s` SET %s WHERE `%s`=?' % (tableName, ','.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
			attrs['__delete__']      = 'DELETE FROM `%s` where `%s`=?' % (tableName, primaryKey)
			return type.__new__(cls, name, bases, attrs)

class Model (dict, metaclass=ModelMetaclass):
	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" % key)
	def __setattr__(self, key, value):
		self[key] = value
	def getValueOrDefault(self, key):
		value = getattr(self, key, None)
		if value is None:
			field = self.__mappings__[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default
				logging.debug('using defualt value for %s: %s' * (key, str(value)))
				setattr(self, key, value)
		return value

	@classmethod
	async def findAll(cls, where=None, args=None, **kw):
		sql = [cls.__select__]
		if where:
			sql.append('where')
			sql.append(where)
		if args is None:
			args = []
		orderBy = kw.get('orderBy', None)	
		if orderBy:
			sql.append('order by')
			sql.append(orderBy)
		limit = kw.get('limit', None)
		if limit is not None:
			sql.append('limit')
			if isinstance(limit, int):
				sql.append('?')
				args.append(limit)
			elif isinstance(limit, tuple) and len(limit) == 2:
				sql.append('?,?')
				args.extend(limit)
			else:
				raise ValueError('Invalid limit value: %s' % str(limit))
		rs = await select(' ', join(sql), args)
		return [cls(**r) for r in rs]
	@classmethod
	async def findNumber(cls, selectField, where=None, args=None):
		sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
		if where:
			sql.append('where')
			sql.append(where)
		rs = await select(' '.join(sql), args, 1)	
		if len(rs)==0:
			return None
		return rs[0]['_num_']
	@classmethod
	async def find(cls, pk):
		rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
		if len(rs)==0:
			return None
		return cls(**rs[0])
	
	async def save(self):
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows = await execute(self.__insert__, args)
		if rows != 1:
			logging.warn('Faild to insert record: affected rows: %s' % rows)

	async def update(self):
		args = list(map(self.getValue, self.__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows = await execute(self.__update__, args)
		if rows != 1:
			logging.warn('Failed to update by primary key: affected rows: %s' % rows)

	async def remove(self):
		args = [self.getValue(self.__primary_key__)]
		rows = await execute(self.__delete__, args)
		if rows != 1:
			logging.warn('Failed to remove by primary key: affected rows %s' % rows)

if __name__ == "__main__":
	# from models import User, Blog, Comment
	class User(Model):
		"""docstring for User"""
		__table__ = 'users'
		id        = StringField(primary_key=True, default="123", ddl='varchar(50)')
		email     = StringField(ddl='varchar(50)')
		passwd    = StringField(ddl='varchar(50)')
		admin     = BooleanField()
		name      = StringField(ddl='varchar(50)')
		image     = StringField(ddl='varchar(500)')
		create_at = FloatField(default=123)

	loop = asyncio.get_event_loop()
	async def test():

		await create_pool(loop=loop, user='www-data', password='www-data', database='awesome')
		u = User(name='Test', email='test@exemple.com', passwd='123456', image='about:blank')
		print (u)
		await u.save()
		print(__pool)
		await destory_pool()
	loop = asyncio.get_event_loop()
	loop.run_until_complete(test())
	
	loop.close()
	if loop.is_closed():
		sys.exit(0)