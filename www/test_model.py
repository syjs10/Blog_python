# from orm import create_pool, destory_pool
# import asyncio
# import aiomysql
# from models import User, Blog, Comment



# async def test(loop):

# 	await create_pool(loop, user='www-data', password='www-data', database='awesome')
# 	# u = User(name='Test', email='test@exemple.com', passwd='123456', image='about:blank')
# 	# await u.save()
# 	print(__pool)
# 	# __pool.close()
# 	await destory_pool()
	
# loop = asyncio.get_event_loop()
# loop.run_until_complete(test(loop))
# # __pool.close()
# # loop.run_until_complete(__pool.wait_closed())

# loop.close()
import asyncio
import logging
import aiomysql
from orm import create_pool
from models import User
logging.basicConfig(level=logging.INFO)

loop = asyncio.get_event_loop()
@asyncio.coroutine
def test2():  #这里使用aiomysql文档的示例写法，直接执行insert语句方式插入数据
    pool = yield from aiomysql.create_pool(host='127.0.0.1', port=3306,
              user='www-data', password='www-data', db='awesome', maxsize=10,minsize=1,loop=loop)
    with (yield from  pool) as conn:
        cursor=yield from conn.cursor()
        user = User(name=f.first_name(),email=f.email(),passwd=f.state(),image=f.company())
        args = list(map(user.getValueOrDefault, user.__fields__))
        args.append(user.getValueOrDefault(user.__primary_key__))
        yield from cursor.execute(user.__insert__.replace('?','%s') , args )
        yield from cursor.close()
        yield from conn.commit()
    pool.close()
    yield from pool.wait_closed()


loop.run_until_complete(test2())
loop.close()