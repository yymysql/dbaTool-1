#!/data1/Python-2.7.4/bin/python
#coding=utf-8
#by jiasir
#mysql 自动处理slave错误,脚本建议本机执行
#autorec.py -s 127.0.0.1 -p xxxx -u xxxx  -pw xxxx
from __future__ import division
import MySQLdb.cursors
import os,sys,glob,re,time
import traceback
import commands
import argparse
import ConfigParser
import yaml
import logging
import logging.handlers

class dbconn():
	'mysql 连接对象'
	def __init__(self,host,port,user,passwd,dbname):
		self.db_host = host
		self.db_port = port
		self.db_user = user
		self.db_passwd = passwd
		self.db_dbname = dbname
		try:
			self.conn = self.getConnection()
			self.conn.select_db(self.db_dbname)
			self.succ=True
		except Exception as error:
			self.succ=False
			print  "Connect mysql error: %s,server ip：%s:%s" % (error,self.db_host,self.db_port)
	def getConnection(self):
		return MySQLdb.connect(host=self.db_host,user=self.db_user,passwd=self.db_passwd,port=int(self.db_port),connect_timeout=5,charset='utf8',cursorclass = MySQLdb.cursors.DictCursor)

	def myquery(self,sql):
		try:
			cursor=self.conn.cursor()
			cursor.execute(sql)
			data=cursor.fetchall()
			cursor.close()
			self.conn.commit()
		except MySQLdb.Warning,w:
			resetwarnings()
		else:
			return data

def logger(Level="debug",LOG_FILE=None):
	'打印日志'
	Loglevel = {"debug":logging.DEBUG,"info":logging.INFO,"error":logging.ERROR,
		"warning":logging.WARNING,"critical":logging.CRITICAL}
	logger = logging.getLogger()
	if LOG_FILE is None:
		hdlr = logging.StreamHandler(sys.stderr)
	else:
		hdlr = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=33554432, backupCount=2)
	formatter = logging.Formatter('%(asctime) s%(lineno)5d %(levelname)s %(message)s','%Y-%m-%d %H:%M:%S')
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr)
	logger.setLevel(Loglevel[Level])
	return logger

def tmp_log(port):
	'记录错误的临时数据'
	tmpdir='/data/mysqldbalogs/autorec/tmpfile'
	if not os.path.exists(tmpdir):
		os.makedirs(tmpdir)
	tmpfile='%s/err_%s_log' % (tmpdir,port)
	try:
		if not os.path.exists(tmpfile):
			ofile=open(tmpfile,'a')
			ofile.write('[info]\n')
			ofile.flush()
			ofile.close
		cp=ConfigParser.ConfigParser(allow_no_value=True)
		cp.read(tmpfile)
		return cp,tmpfile
	except Exception, error:
		print error
		return False,''


def get_masterconn(myconn,slaveinfo):
	'获取master密码及master连接'
	masterhost=slaveinfo[0]['Master_Host']
	masterport=slaveinfo[0]['Master_Port']
	masteruser=slaveinfo[0]['Master_User']
	version=myconn.myquery('select version() as version')[0]['version']
	####由于5.5 show slave status 没有 Master_Info_File
	if version.startswith('5.5'):
		datadir=myconn.myquery('show variables like "datadir"')[0]['Value']
		passwordfile='%s/master.info' % (datadir)
	else:
		passwordfile=slaveinfo[0]['Master_Info_File']
	cmd="sed -n '6p' %s" % (passwordfile)
	try:
		if os.path.isfile(passwordfile):
			masterpassword=commands.getstatusoutput(cmd)[1]
		else:
			masterpassword=myconn.myquery('select User_password from mysql.slave_master_info')[0]["User_password"]
	except Exception, error:
		lg.info('get master password error: %s' %(error))
	masterconn=dbconn(masterhost,masterport,masteruser,masterpassword,dbname)
	lg.info('connect master succ...')
	return masterpassword,masterconn

def err_idempotent(myconn,port):
	'针对错误1032,1062,1452'
	lg.info('begin fix 1032,1062,1452')
	try:
		myconn.myquery("set global slave_exec_mode='IDEMPOTENT'")
		myconn.myquery("stop slave")
		##此处收集错误信息
		slaveinfo=myconn.myquery("show slave status")
		err_log(myconn,port,slaveinfo[0]['Last_SQL_Errno'],'set global slave_exec_mode="IDEMPOTENT"')
		myconn.myquery("start slave")
		lg.info('fix 1032,1062,1452 succ')
	except Exception, error:
		lg.info('fix 1032,1062,1452 fail: %s' % (error))
		#print "执行set global slave_exec_mode='IDEMPOTENT'错误"
		#print error 

def err_1051(myconn,port):
	'1051 drop 表不存在错误'
	lg.info('begin fix 1051')
	try:
		myconn.myquery("stop slave")
		slaveinfo=myconn.myquery("show slave status")
		err_log(myconn,port,slaveinfo[0]['Last_SQL_Errno'],'set global sql_slave_skip_counter=1')
		myconn.myquery("set global sql_slave_skip_counter=1")
		myconn.myquery("start slave")
		lg.info('fix 1051 succ')
	except Exception, error:
		lg.info('fix 1051 fail: %s' % (error))
		#print "执行set global sql_slave_skip_counter=1错误"
		#print error

def err_1146(myconn,port,slaveinfo):
	'表不存在1146错误的修复'
	#print "error 1146"
	lg.info('begin fix 1146')
	try:
		mapasswd,masterconn=get_masterconn(myconn,slaveinfo)
		if not masterconn.succ:
			print "master connect error"
			return 100
		sql_error=slaveinfo[0]['Last_SQL_Error']
		tablenames=re.findall(r"Table(.+?)doesn't exist",sql_error)
		if len(tablenames)>=1:
			tablename=format(tablenames[0].replace("'",""))
			tableschema=tablename[:tablename.index(".")]
		create_table_sql="show create table %s" % (tablename)
		create_table=masterconn.myquery(create_table_sql)
		if len('create_table')>=1:
			create_sql=re.sub(r"^CREATE TABLE","CREATE TABLE IF NOT EXISTS",create_table[0]["Create Table"],count=1)
			#print create_sql
			myconn.myquery("stop slave")
			##收集错误信息
			slaveinfo=myconn.myquery("show slave status")
			err_log(myconn,port,slaveinfo[0]['Last_SQL_Errno'],create_sql)
			if len(myconn.myquery("use %s" % (tableschema)))==0:
				myconn.myquery(create_sql)
			myconn.myquery("start slave")
			lg.info('fix 1146 succ')
	except Exception, error:
		lg.info('fix 1146 fail: %s' % (error))

def err_1197(myconn,port,slaveinfo):
	'1197 max_binlog_cache_size 错误'
	lg.info('begin fix 1197')
	try:
		mapasswd,masterconn=get_masterconn(myconn,slaveinfo)
		if not masterconn.succ:
			print "master connect error"
			return 100
		max_binlog_cache_size=masterconn.myquery('show variables like "max_binlog_cache_size"')
		if len(max_binlog_cache_size)>0:
			myconn.myquery("stop slave")
			##收集错误信息
			max_binlog_cache_size_value=max_binlog_cache_size[0]['Value']
			set_variable_sql="set global max_binlog_cache_size=%s" % (max_binlog_cache_size_value)
			slaveinfo=myconn.myquery("show slave status")
			err_log(myconn,port,slaveinfo[0]['Last_SQL_Errno'],set_variable_sql)
			myconn.myquery(set_variable_sql)
			myconn.myquery("start slave")
			lg.info('fix 1197 succ')
	except Exception, error:
		lg.info('fix 1197 fail: %s' % (error))

def err_1236(myconn,port,slaveinfo):
	'1236 日志点错误修复,遇到日志点错误,到master找出目前复制的日志文件Relay_Master_Log_File的最接近切小于Exec_Master_Log_Pos的点进行重新连接,并开启幂等'
	lg.info('begin fix 1236')
	try:
		mapasswd,masterconn=get_masterconn(myconn,slaveinfo)
		if not masterconn.succ:
			print "master connect error"
			return False
		Relay_Master_Log_File_first=slaveinfo[0]['Relay_Master_Log_File']
		Relay_Master_Log_File=slaveinfo[0]['Relay_Master_Log_File']
		execpos=slaveinfo[0]['Exec_Master_Log_Pos']
		try:
			position=0
			binlog_event_sql="show binlog events in '{}'".format(Relay_Master_Log_File)
			binlog_event=masterconn.myquery(binlog_event_sql)
			blen=len(binlog_event)-1
			for one in range(blen,-1,-1):
				if binlog_event[one]['End_log_pos']<=execpos:
					'找出第一个小于execpos的日志点'
					position=binlog_event[one]['End_log_pos']
					break
		except Exception, error:
			lg.info(error)
			'如果上述出错,可能由于日志损坏show binlog events错误或者其他错误.所以直接跳到下一个日志'
			binlog_list=masterconn.myquery('show master logs;')
			binlog_list_len=len(binlog_list)
			count=0
			while count>0:
				if binlog_list[count]['Log_name']==Relay_Master_Log_File_first:
					Relay_Master_Log_File=binlog_list[count+1]['Log_name']
					position=0
					break
		change_sql="change  MASTER TO  MASTER_LOG_FILE='{}',MASTER_LOG_POS={};".format(Relay_Master_Log_File,position)
		myconn.myquery("stop slave")
		slaveinfo=myconn.myquery("show slave status")
		err_log(myconn,port,slaveinfo[0]['Last_IO_Errno'],change_sql)
		myconn.myquery('set global slave_exec_mode="IDEMPOTENT"')
		lg.info(change_sql)
		myconn.myquery(change_sql)
		myconn.myquery("start slave")
		time.sleep(5)
		slaveinfo=myconn.myquery("show slave status")
		if slaveinfo[0]['Last_IO_Errno']==1236:
			'防止不断跳过整个日志,如果跳过不成功,则直接stop slave'
			myconn.myquery("stop slave")
			lg.info('sql1236 stop slave')
			lg.info('fix 1236 fail')
		else:
			lg.info('fix 1236 succ')
	except Exception, error:
		lg.info('fix 1236 fail: %s' % (error))

def err_1594(myconn,port,slaveinfo):
	'中继日志读取错误Relay log read failure,Last_SQL_Errno可能原因是从库死掉,重启发生错误,处理方法,重新change master,如果不信reset 后change master'
	lg.info('begin fix 1594')
	try:
		Relay_Master_Log_File=slaveinfo[0]['Relay_Master_Log_File']
		execpos=slaveinfo[0]['Exec_Master_Log_Pos']
		Master_Host=slaveinfo[0]['Master_Host']
		Master_User=slaveinfo[0]['Master_User']
		Master_Port=slaveinfo[0]['Master_Port']
		change_sql="change  MASTER TO  MASTER_LOG_FILE='{}',MASTER_LOG_POS={};".format(Relay_Master_Log_File,execpos)
		lg.info(change_sql)
		myconn.myquery("stop slave")
		slaveinfo=myconn.myquery("show slave status")
		err_log(myconn,port,slaveinfo[0]['Last_SQL_Errno'],change_sql)
		myconn.myquery(change_sql)
		myconn.myquery("start slave")
		time.sleep(5)
		slaveinfo=myconn.myquery("show slave status")
		if slaveinfo[0]['Last_SQL_Errno']==1594:
			'如果还是同样错误,则reset slave all'
			Relay_Master_Log_File=slaveinfo[0]['Relay_Master_Log_File']
			execpos=slaveinfo[0]['Exec_Master_Log_Pos']
			Master_Host=slaveinfo[0]['Master_Host']
			Master_User=slaveinfo[0]['Master_User']
			Master_Port=slaveinfo[0]['Master_Port']
			mapasswd,masterconn=get_masterconn(myconn,slaveinfo)
			change_sql="change  MASTER TO  MASTER_LOG_FILE='{}',MASTER_LOG_POS={},MASTER_HOST='{}',MASTER_PORT={},MASTER_USER='{}',MASTER_PASSWORD='{}';".format(Relay_Master_Log_File,execpos,Master_Host,Master_Port,Master_User,mapasswd)
			lg.info(change_sql)
			myconn.myquery("stop slave")
			slaveinfo=myconn.myquery("show slave status")
			err_log(myconn,port,slaveinfo[0]['Last_SQL_Errno'],change_sql)
			myconn.myquery(change_sql)
			myconn.myquery("start slave")
			lg.info('fix 1594 succ')
	except Exception, error:
		lg.info('fix 1594 fail: %s' % (error))


def err_1677(myconn,port,slaveinfo):
	'修复部分1677错误,错误主要由于双主结构下对一端DDL另一端DML导致.修复为跳过记录,即不一致临时修复,后期需要一致性校验修复数据.再跳过错误前比对主从节点表结构和版本是否已经一致'
	lg.info('begin fix 1677')
	cp,tmpfile=tmp_log(port)
	#记录修复错误次数,每天超过5次,不能修复不再修复
	try:
		cp,tmpfile=tmp_log(port)
		if cp:
			info=cp.items('info')
			nowtime=int(time.time())
			if cp.has_option('info','time_1677'):time_1677=int(cp.get('info','time_1677'))
			else:time_1677=nowtime
			if cp.has_option('info','errcount_1677'):errcount_1677=int(cp.get('info','errcount_1677'))
			else:errcount_1677=0
			diff_time=nowtime-time_1677
			time_interval=3600
			if diff_time<time_interval and errcount_1677>=5:
				lg.info('1677 only fix 5 times per hour')
				return False
			if diff_time<time_interval:
				errcount_1677=errcount_1677+1
			if diff_time>time_interval:
				time_1677=nowtime
				errcount_1677=0
			cp.set('info','errcount_1677',errcount_1677)
			cp.set('info','time_1677',time_1677)
			ofile=open(tmpfile,'w')
			cp.write(ofile)
			ofile.flush()
	except Exception, error:
		lg.info('fix 1677 fail(open tmpfile): %s' % (error))
		return False

	try:
		mapasswd,masterconn=get_masterconn(myconn,slaveinfo)
		if not masterconn.succ:
			lg.info('fix 1677 fail(link master error)')
			return False
		#过滤出table名称
		sql_error=slaveinfo[0]['Last_SQL_Error']
		tablenames=re.findall(r"Column.+of table(.+?)cannot be converted from type",sql_error)
		column=re.findall(r"Column(.+?)of table.+cannot be converted from type",sql_error)
		columntype=re.findall(r"Column.+of table.+cannot be converted from type (.+) to type (.+)",sql_error)
		if len(column)==1:
			columnpos=int(format(column[0].replace("'","")).strip())
			columnpos=columnpos+1
		else:
			return False
		if len(tablenames)==1:
			tablename=format(tablenames[0].replace("'","")).strip()
			tableshortname=tablename[tablename.index(".")+1:].strip()
			tableschema=tablename[:tablename.index(".")].strip()
		else:
			return False
		table_sql="select concat(IFNULL(version(),''),IFNULL(t.ENGINE,''),IFNULL(ROW_FORMAT,''),IFNULL(TABLE_COLLATION,''),c.COLUMN_NAME,c.ORDINAL_POSITION,IFNULL(c.COLUMN_DEFAULT,''),c.IS_NULLABLE,c.DATA_TYPE,IFNULL(c.CHARACTER_MAXIMUM_LENGTH,''),IFNULL(c.CHARACTER_OCTET_LENGTH,''),IFNULL(c.NUMERIC_PRECISION,''),IFNULL(c.NUMERIC_SCALE,''),IFNULL(c.DATETIME_PRECISION,''),IFNULL(c.EXTRA,''),IFNULL(c.CHARACTER_SET_NAME,''),IFNULL(c.COLLATION_NAME,''),IFNULL(c.COLUMN_TYPE,''),IFNULL(c.COLUMN_KEY,'')) AS COL from information_schema.columns  c  inner join information_schema.TABLES t ON c.TABLE_SCHEMA=t.TABLE_SCHEMA and c.TABLE_NAME=t.TABLE_NAME  where c.TABLE_SCHEMA='%s' and c.TABLE_NAME='%s'  ORDER BY c.ORDINAL_POSITION;" % (tableschema,tableshortname)
		ma_tabletxt=masterconn.myquery(table_sql)
		my_tabletxt=myconn.myquery(table_sql)
		malen=len(ma_tabletxt)
		if malen>=1 and malen==len(my_tabletxt):
			tablesame=1
			one=0
			while one<malen:
				if ma_tabletxt[one]['COL']!=my_tabletxt[one]['COL']:
					tablesame=0	
					break
				one=one+1
			if tablesame==0:
				lg.info('fix 1677 fail(Table Structure diff between master and slave)')
			if tablesame==1:
				#尝试重启是否可以恢复
				lg.info('trying restart to fix 1677')
				myconn.myquery("stop slave")
				myconn.myquery("start slave")
				time.sleep(1)
				info1=myconn.myquery("show slave status")
				if len(info1)==1:
					if info1[0]['Last_SQL_Errno']!=1677:
						lg.info('fix 1677 succ(restart)')
						return True
					elif info1[0]['Exec_Master_Log_Pos']==slaveinfo[0]['Exec_Master_Log_Pos']:
						#如果重启依然有错误,且日志点没变,说明重启不能恢复,则执行跳过错误
						lg.info('trying skipcount=1 to fix 1677')
						myconn.myquery("stop slave")
						slaveinfo=myconn.myquery("show slave status")
						err_log(myconn,port,slaveinfo[0]['Last_SQL_Errno'],'set global sql_slave_skip_counter=1')
						myconn.myquery("set global sql_slave_skip_counter=1")
						myconn.myquery("start slave")
						lg.info('fix 1677 succ(skipcount)')
						
	except Exception, error:
		lg.info('fix 1677 fail: %s' % (error))

def error_fixed(myconn,port,slaveinfo):
	'错误修复控制'
	if len(slaveinfo)==0:
		lg.info("master node")
		return 2
	if slaveinfo[0]['Last_SQL_Errno']==0 and slaveinfo[0]['Last_IO_Errno']==0:
		lg.info("no error")
		return 0
	##部分修复要保证GTID不开启情况下
	gtid=myconn.myquery("show variables like 'gtid_mode';")
	gtidval=True
	if len(gtid)==1:
		if gtid[0]['Value']=='ON' or gtid[0]['Value']==1:
			gtidval=False
		
	#故障处理
	if slaveinfo[0]['Last_SQL_Errno'] in (1032,1062,1452):
		err_idempotent(myconn,port)
	if slaveinfo[0]['Last_SQL_Errno'] in (1051,1050) and gtidval:
		err_1051(myconn,port)
	if slaveinfo[0]['Last_SQL_Errno']==1146:
		err_1146(myconn,port,slaveinfo)
	if slaveinfo[0]['Last_SQL_Errno']==1197:
		err_1197(myconn,port,slaveinfo)
	if slaveinfo[0]['Last_IO_Errno']==1236:
		err_1236(myconn,port,slaveinfo)
	if slaveinfo[0]['Last_SQL_Errno']==1594:
		err_1236(myconn,port,slaveinfo)
	if slaveinfo[0]['Last_SQL_Errno']==1677 and gtidval:
		err_1677(myconn,port,slaveinfo)

##修复错误前记录错误日志
def err_log(myconn,port,err,msg):
	'处理错误时记录日志'
	slaveinfo=myconn.myquery("show slave status")
	if len(slaveinfo)>0:
		slaveinfostr="""
Master_Host:%s
Master_Port:%s
Master_Log_File:%s
Read_Master_Log_Pos:%s
Relay_Log_File:%s
Relay_Log_Pos:%s
Relay_Master_Log_File:%s
Exec_Master_Log_Pos:%s
Slave_IO_Running:%s
Slave_SQL_Running:%s
Last_IO_Errno:%s
Last_IO_Error:%s
Last_IO_Error_Timestamp:%s
Last_SQL_Errno:%s
Last_SQL_Error:%s
Last_SQL_Error_Timestamp:%s
Seconds_Behind_Master:%s
""" % (slaveinfo[0]['Master_Host'],slaveinfo[0]['Master_Port'],slaveinfo[0]['Master_Log_File'],slaveinfo[0]['Read_Master_Log_Pos'],slaveinfo[0]['Relay_Log_File'],slaveinfo[0]['Relay_Log_Pos'],slaveinfo[0]['Relay_Master_Log_File'],slaveinfo[0]['Exec_Master_Log_Pos'],slaveinfo[0]['Slave_IO_Running'],slaveinfo[0]['Slave_SQL_Running'],slaveinfo[0]['Last_IO_Errno'],slaveinfo[0]['Last_IO_Error'],slaveinfo[0]['Last_IO_Error_Timestamp'],slaveinfo[0]['Last_SQL_Errno'],slaveinfo[0]['Last_SQL_Error'],slaveinfo[0]['Last_SQL_Error_Timestamp'],slaveinfo[0]['Seconds_Behind_Master'])
	else:
		slaveinfostr=""
	logdir='/data/mysqldbalogs/autorec'
	datetime=time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))
	if not os.path.exists(logdir):
		os.makedirs(logdir)
	errorfile='%s/err_%s_%s_%s' % (logdir,port,err,datetime)
	try:
		ofile=open(errorfile,'a')
		ofile.write(slaveinfostr)
		msg=msg.encode('utf-8')
		ofile.write(msg)
		ofile.close
		lg.info('logging slave info succ')
	except Exception, error:
		ofile.close
		lg.info('logging slave info error: %s' % (error))

def check_slave_status(myconn):
	'检查slave的状态'
	try:
		slaveinfo=myconn.myquery("show slave status")
		if len(slaveinfo)==1:
			if (slaveinfo[0]['Slave_IO_Running']=='Yes'  and slaveinfo[0]['Last_SQL_Errno'] in (1032,1062,1594,1452,1146,1051,1050,1197,1677)):
				return slaveinfo[0]['Last_SQL_Errno']
			elif (slaveinfo[0]['Slave_SQL_Running']=='Yes' and slaveinfo[0]['Last_IO_Errno'] in (1236,)):
				return slaveinfo[0]['Last_IO_Errno']
		#lg.info('check slave status succ')
		return  0
	except Exception, error:
		lg.info('check slave status error: %s' % (error))
		return 0

def grant_pri(myconn):
	'每次检查操作的时候检查slave repl账号是否有 Show view 权限'
	try:
		###跑脚本的时候顺便确保本机的复制账号给予修复时候需要的权限
		select_user_sql="select user,host from mysql.user where (Show_view_priv='N' or Select_priv='N' or Reload_priv='N' ) and user in ('slave','repl','repli')"
		select_user=myconn.myquery(select_user_sql)
		for one in select_user:
			grantpri_sql='grant Show view,select,Reload on *.* to "{}"@"{}"'.format(one['user'],one['host'])
			myconn.myquery(grantpri_sql)
			lg.info('grant privileges %s@%s succ' % (one['user'],one['host']))
	except Exception, error:
		lg.info('grant privileges error: %s' % (error))

def mainpro(host,port,user,password):
	try:
		###获取数据库连接
		myconn=dbconn(host,port,user,password,dbname)
		if not myconn.succ:
			lg.info('mysql %s:%s link error' % (host,port))
			return
		grant_pri(myconn)
		slaveinfo=myconn.myquery("show slave status")
		for num in range(1,5):
			slave_status=check_slave_status(myconn)
			if slave_status==0:
				if num >1:
					lg.info('send fix succ msg')
				lg.info('check %s %s finish'%(host,port))
				break
			if num==1:
				lg.info('send fix alarm msg')
			lg.info('%s time check and fix %s %s'%(num,host,port))
			slaveinfo=myconn.myquery("show slave status")
			error_fixed(myconn,port,slaveinfo)
			time.sleep(1)

	except Exception, error:
		lg.info('mainpro exec error: %s' % (error))


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='*****mysql slave error auto resolve*****')
	parser.add_argument('-s','--host',type=str,default='127.0.0.1',required=False,help='mysql instance ip,default:127.0.0.1')
	parser.add_argument('-p','--port', type=int,required=True,help="mysql instance port")
	parser.add_argument('-u','--user',type=str,default='root',required=False,help='mysql user')
	parser.add_argument('-pw','--passwd',type=str,default='',required=False,help='mysql password')
	args = parser.parse_args()
	port = args.port
	user = args.user
	host = args.host
	passwd = args.passwd
	dbname='information_schema'
	lg = logger("info")
	mainpro(host,port,user,passwd)
