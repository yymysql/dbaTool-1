#!/bin/python
#coding=utf-8
#by jiasir
import datetime
import pymongo
import argparse
import commands
import os,sys,glob,re,time
import ConfigParser
from tempfile import TemporaryFile
class mymongo:
	host=None
	port=None
	passwd=None
	user=None

	def __init__(self,host,port,user,passwd,authdb):
		self.host=host
		self.port=port
		self.passwd=passwd
		self.user=user
		self.authdb=authdb
		try:
			self.conn=self.mongo_conn()
			if self.conn:
				try:
					self.info=self.conn.server_info()
					self.succ=True
				except Exception as error:
					self.succ=False
					print error
			else:
				self.succ=False
		except Exception as error:
			print error
			print  "Connect mongo error: %s,server ip:%s:%s" % (error,self.host,self.port)
			self.succ=False
	   
	def mongo_conn(self):
		try:
			#self.connection=pymongo.MongoClient(host=self.host, port=int(self.port),serverSelectionTimeoutMS=3)
			#uri='mongodb://' + user + ':' + pwd + '@' + server + ':' + port +'/'+ db_name
			uri="mongodb://%s:%s@%s:%s/%s" % (self.user,self.passwd,self.host,self.port,self.authdb)
			#print uri
			self.connection=pymongo.MongoClient(uri)
			#self.connection=pymongo.MongoClient(host=self.host, port=int(self.port),serverSelectionTimeoutMS=3)
			return self.connection
		except Exception as error:
			return False
			print 'mongo  connect fail %s' % (error)
	def admin_comm(self,stmt):
		return self.conn.admin.command(stmt)


def current_op(myconn,opquery):
	'获取当前running 线程的信息'
	db=myconn.conn['local']
	current_op=db.current_op()
	loglist=current_op.get("inprog",[])
	title="connid-----client------------|--op----|microS---------|Lwait|--namespace-------------------|--------------query---------------->filter------>sort-------"
	count=0
	for one in loglist:
		if count%20==0:
			print title
		id=one.get('connectionId',0)
		count=count+1
		if id==0:
			continue
			count=count-1
		onemsg="%-7s %-21s|%-8s|%-15s|%-5s|%-30s|" % (id,one.get('client',0),one['op'],one.get('microsecs_running',0),one['waitingForLock'],one['ns'])
		querymsg=str(one.get('query',{}).keys())
		filtermsg=str(one.get('query',{}).get('filter',{}).keys())
		sortmsg=str(one.get('query',{}).get('sort',{}).keys())
		querymsg=querymsg+">"+filtermsg+">"+sortmsg
		print onemsg+"%-60s" % (querymsg)
		if opquery:
			print "    "+str(one)
#####################

def slow_log(myconn,dbname,limit,slowlogquery,slowloguser):
	'获取慢日志'
	print "[ {} ]:".format(dbname)
	db=myconn.conn[dbname]
	collection=db.system.profile
	log=collection.find({"$and": [{"op":{ "$ne":'command'}}]}).sort([('ts',-1)]).limit(limit)
	title="op------|-time-------------millis--|keyE-docE----keyU-----wrCon--return--Kbytes---nscan----nscanObj-|-stage---Istage--|---namespace--------|"
	if slowlogquery:
		 title=title+'--------------------query------------>filter------>sort-----|'
	if slowloguser:
		title=title+'------client---------user----------|'
	count=0
	for one in log:
		if count%20==0:
			print title
		ts=str(one['ts']+datetime.timedelta(hours=8))[0:19]
		generalmsg="%-8s|%-20s %-5s|%-4s %-8s %-8s %-5s %-8s %-8s %-8s %-8s|%-8s %-8s|%-20s|" %(one['op'],ts,one['millis'],one.get('keysExamined',0),one.get('docsExamined',0),one.get('keyUpdates',0),one.get('writeConflicts',0),one.get('nreturned',0),one.get('responseLength',0)/1024,one.get('nscanned',0),one.get('nscannedObjects',0),one.get('execStats',{}).get('stage',0),one.get('execStats',{}).get('inputStage',{}).get('stage',0),one['ns'].split('.')[1])
		if slowlogquery:
			querymsg=str(one.get('query',{}).keys())
			filtermsg=str(one.get('query',{}).get('filter',{}).keys())
			sortmsg=str(one.get('query',{}).get('sort',{}).keys())
			querymsg=querymsg+">"+filtermsg+">"+sortmsg
			generalmsg=generalmsg+'%-60s|' % (querymsg)
		if slowloguser:
			generalmsg=generalmsg+"%-16s %-18s|" % (one.get('client','none'),one.get('user','none'))
		print generalmsg
		count=count+1
def slow_log_main(myconn,dbname,limit,slowlogquery,slowloguser):
	'慢日志'
	if dbname=='':
		alldb=myconn.conn.database_names()
	else:
		alldb=dbname.split()
	for one in alldb:
		if one not in ('local','admin'):
			slow_log(myconn,one,limit,slowlogquery,slowloguser)

############################

def list_db(myconn):
	'列出所有db'
	try:
		alldb=myconn.conn.database_names()
		for one in alldb:
			print one
	except Exception, error:
		print error

######################
def list_coll(myconn,dbname):
	'列出DB下的collection'
	print "[ {} ]:".format(dbname)
	db=myconn.conn[dbname]
	all_coll=db.collection_names()
	for one in all_coll:
		print "    "+one

def list_coll_main(myconn,dbname):
	'列出collection的主程序'
	if dbname=='':
		alldb=myconn.conn.database_names()
	else:
		alldb=dbname.split()
	for one in alldb:
		list_coll(myconn,one)
#######################

def count_coll(myconn,dbname):
	'返回每个集合的条目'
	try:
		print "[ {} ]:".format(dbname)
		db=myconn.conn[dbname]
		all_coll=db.collection_names()
		for one in all_coll:
			onecol=db[one]
			col_count=onecol.find().count()
			print "%-30s %-30s" % (one,col_count)
	except Exception, error:
		print error

def count_coll_main(myconn,dbname):
	if dbname=='':
		alldb=myconn.conn.database_names()
	else:
		alldb=dbname.split()
	for one in alldb:
		if one not in ('local','admin'):
			count_coll(myconn,one)
######################
	
def user_list(myconn):
	'列出数据库用户列表'
	try:
		db=myconn.conn['admin']
		col=db['system.users']
		userlist=col.find()
		for one in userlist:
			msg=""
			msg="""[ user_db: {} {} ]""".format(one['user'],one['db'])
			roles=""
			for onerole in one["roles"]:
				roles=roles+"\ndb:%-12s role:%-20s" % (onerole['db'],onerole["role"])
			allmsg=msg+roles+"\n"
			print allmsg
	except Exception, error:
		print error
	
def repl_status(myconn):
	'查找复制集信息'
	try:
		db=myconn.conn['admin']
		replstatus=db.command("replSetGetStatus")
		print "[ {} ]".format(replstatus["set"])
		title='---member---------------syncingTo--------stateStr--health-uptime---optimeTs----------------'
		print title
		for one in replstatus["members"]:
			onemsg="%-20s %-20s %-10s %-2s %-8s %-20s" % (one['name'],one.get('syncingTo',0),one["stateStr"],one["health"],one["uptime"],one.get('optime',{}).get('ts',0))
			print onemsg
	except Exception, error:
		print error

def repl_conf(myconn):
	'查找复制集的配置信息'
	try:
		db=myconn.conn['admin']
		replconf=db.command("replSetGetConfig")
		print "[ {} ]".format(replconf["config"]["_id"])
		title='id---member-------------hidden-pri--vote--bIdx--Arb---slaveDel-'
		print title
		for one in replconf["config"]["members"]:
			onemsg="%-2s %-20s %-5s %-5s %-5s %-5s %-5s %-8s " % (one['_id'],one['host'],one['hidden'],one['priority'],one['votes'],one['buildIndexes'],one['arbiterOnly'],one['slaveDelay'])
			print onemsg
	except Exception, error:
		print error
	
	

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='mgongo tools')
	parser.add_argument('-p','--port', type=int,required=True,help="指定实例端口")
	parser.add_argument('-u','--user',type=str,default='root',required=False,help='登陆用户')
	parser.add_argument('-s','--host',type=str,default='127.0.0.1',required=False,help='登陆IP,默认127.0.0.1')
	parser.add_argument('-pw','--passwd',type=str,default='',required=False,help='登陆密码')
	parser.add_argument('-a','--authdb',type=str,default='admin',required=False,help='验证db,默认admin')
	parser.add_argument('-db','--dbname',type=str,default='',required=False,help='数据库名称')
	parser.add_argument('-lm','--limit', type=int,required=False,default=9999999999,help="限制查询慢日志的条数,默认所有")
	parser.add_argument('-sl','--slowlog', action='store_true',default=False,help='查看慢日志')
	parser.add_argument('-slq','--slowlogquery', action='store_true',default=False,help='查看慢日志是否展示query信息')
	parser.add_argument('-slu','--slowloguser', action='store_true',default=False,help='查看慢日志是否显示连接用户')
	parser.add_argument('-c','--count', action='store_true',default=False,help='统计collction的文档数')
	parser.add_argument('-lu','--userlist', action='store_true',default=False,help='列出系统用户及权限')
	parser.add_argument('-co','--currentop', action='store_true',default=False,help='查看当前running的线程信息')
	parser.add_argument('-coq','--opquery', action='store_true',default=False,help='查看当前running的线程信息且打印出语句')
	parser.add_argument('-ld','--listdb', action='store_true',default=False,help='列出所有db')
	parser.add_argument('-lc','--listcoll', action='store_true',default=False,help='列出指定db或所有db的collection')
	parser.add_argument('-rs','--replstatus', action='store_true',default=False,help='查看rs.status')
	parser.add_argument('-rc','--replconf', action='store_true',default=False,help='查看rs.config')
	args = parser.parse_args()
	fileport = args.port
	host=args.host
	user = args.user
	passwd = args.passwd
	authdb = args.authdb
	slowlog = args.slowlog
	slowloguser = args.slowloguser
	slowlogquery = args.slowlogquery
	count=args.count
	userlist=args.userlist
	currentop=args.currentop
	opquery=args.opquery
	listdb=args.listdb
	listcoll=args.listcoll
	replstatus=args.replstatus
	replconf=args.replconf
	dbname=args.dbname
	limit=args.limit
	myconn=mymongo(host,fileport,user,passwd,authdb)
	if myconn.succ:
		if slowlog:
			slow_log_main(myconn,dbname,limit,slowlogquery,slowloguser)
		if count:
			count_coll_main(myconn,dbname)
		if userlist:
			user_list(myconn)
		if currentop:
			current_op(myconn,opquery)
		if replstatus:
			repl_status(myconn)
		if replconf:
			repl_conf(myconn)
		if listdb:
			list_db(myconn)
		if listcoll:
			list_coll_main(myconn,dbname)
			

