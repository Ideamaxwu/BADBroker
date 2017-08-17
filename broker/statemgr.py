#!/usr/bin/env python3

import tornado.gen
from brokerobjects import *

log = brokerutils.setup_logging(__name__)

class StateMgr():
	stateMgrInstance = None
	
	@classmethod
	def getInstance(cls):
		if StateMgr.stateMgrInstance is None:
			StateMgr.stateMgrInstance = StateMgr()
		return StateMgr.stateMgrInstance
		
	def __init__(self):
		log.info("StateMgr start.")
		self.asterix = AsterixQueryManager.getInstance()
		self.stateDict = {}
	# state capture
	def Capture(self, dataverse, dataset, stateData):
		log.info("StateMgr capture. > " + dataverse + "." + dataset)
		if dataverse+'.'+dataset in self.stateDict:
			print('stateData has in stateDict')
		else:
			self.stateDict[dataverse+'.'+dataset] = stateData
		
	# state store
	@tornado.gen.coroutine
	def Store(self):
		log.info("StateMgr store.")
		
		dataverseName = "StateSession"
		datasetName = "BrokerStates"
		record = {"brokerName":"brokertest","brokerIp":"128.123.9.10","brokerPort":"9810"}
		sqlpp_stmt = "upsert into {0}({1})".format(datasetName, record)
		status_code, response = yield self.asterix.executeSQLPP(dataverseName, sqlpp_stmt)
		if status_code != 200:
			raise BADException(response)
		log.info('State upsert into %s' %datasetName)
		
		'''
		for k, v in self.stateDict.items():
			dataverseName, datasetName = k.split(".")
			for ik, iv in v.items():
				record = iv
				sqlpp_stmt = "upsert into {0}({1})".format(datasetName, record)
				status_code, response = yield self.asterix.executeSQLPP(dataverseName, sqlpp_stmt)
				if status_code != 200:
					raise BADException(response)
				log.info('State upsert into %s' %datasetName)
		'''

	# state migration
	def Migration(self, initBroker, userList):
		log.info("StateMgr migration.")
		
		# MigrateUser @initBroker
		print('Migrate Users ' + str(userList) + ' from Broker ' + initBroker)
		lenOfBrokers = len(self.stateDict['StateSession.BrokerStates'])-1
		uIndex = 0
		targetBrokerSet = {b:self.stateDict['StateSession.BrokerStates'][b] for b in self.stateDict['StateSession.BrokerStates'] if b!=initBroker}
		for user in userList:
			targetBroker = list(targetBrokerSet.keys())[uIndex%lenOfBrokers]
			print('Moving User ' + user + ' to Broker ' + str(targetBroker))
			
			#TODO send request to targetBroker
			
			uIndex += 1
		# UserSwitch @initUser
		#TODO similar with MigrateUser, but broker selection policy is different
		
	# state recovery
	def Recovery(self, deadBroker):
		log.info("StateMgr recovery.")
		
		print("Broker " + deadBroker + " is recovering...")
		# NewBroker
		#TODO run script to start a new broker, SQLPP all the users of the dead broker, and migrate them to it
		
		# DistributeToBrokers
		#TODO SQLPP all the users of the dead broker, and migrate them to them
		'''
		
		dataverseName = "StateSession"
		datasetName = "BrokerStates"
		record = {"brokerName":"brokertest","brokerIp":"128.123.9.10","brokerPort":"9810"}
		sqlpp_stmt = "upsert into {0}({1})".format(datasetName, record)
		status_code, response = yield self.asterix.executeSQLPP(dataverseName, sqlpp_stmt)
		if status_code != 200:
			raise BADException(response)
		log.info('State upsert into %s' %datasetName)
		
		'''
	# state status
	def Status(self):
		log.info("StateMgr status.")
		if len(self.stateDict) == 0:
			print("No state.")
		else:
			for k,v in self.stateDict.items():
				print(k + ': ' + str(v))
			
if __name__ == "__main__":
	
	brokers = {'broker1':{'ip':'128.195.4.21'}}
	clients = {'client1':{'name':'yao'}}
	
	stateMgr = StateMgr.getInstance()
	stateMgr.Status()
	
	stateMgr.Capture('StateSession', 'BrokerStates', brokers)
	stateMgr.Capture('StateSession', 'BrokerStates', brokers) # duplicate state test
	stateMgr.Capture('StateSession', 'ClinetStates', clients)
	stateMgr.Status()
	
	brokers['broker2'] = {'ip':'128.195.80.21'} # new state item test
	stateMgr.Status()
	brokers['broker2'] = {'ip':'128.195.80.88'} # state item change test
	stateMgr.Status()
	
	stateMgr.Store()
	
	stateMgr.Migration('broker1', ['client1','client2','client3'])
	
	stateMgr.Recovery('broker1')
