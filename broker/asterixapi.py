#!/usr/bin/env python3

import requests
import urllib.parse
import tornado.httpclient
import logging as log

log.getLogger(__name__)
log.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=log.DEBUG)

class AsterixQueryManager():
    def __init__(self, baseURL, servicePoint='query'):
        self.asterixBaseURL = baseURL
        self.asterixServicePoint = servicePoint
        self.queryString = ""
        self.dataverseName = None
    
    def setServicePoint(self, servicePoint):
        self.asterixServicePoint = servicePoint
        
    def setDataverseName(self, dataverseName):
        self.dataverseName = dataverseName
        return self
                            
    def forClause(self, clause):
        if self.dataverseName is None:
            raise Exception('No dataverse name set')
            
        self.queryString = self.queryString + " for  " + clause
        return self
        
    def letClause(self, clause):
        if self.dataverseName is None:
            raise Exception('No dataverse name set')

        if len(self.queryString) == 0:
            raise Exception("LET cann't start a query")
        else:
            self.queryString = self.queryString + " let  " + clause
        return self
     
    def whereClause(self, clause):
        if self.dataverseName is None:
            raise Exception('No dataverse name set')

        if len(self.queryString) == 0:
            raise Exception("WHERE cann't start a query")
        else:
            self.queryString = self.queryString + " where  " + clause
            
        return self

    def orderByClause(self, clause):
        if self.dataverseName is None:
            raise Exception('No dataverse name set')

        if len(self.queryString) == 0:
            raise Exception("ORDER BY cann't start a query")
        else:
            self.queryString = self.queryString + " order by  " + clause
        
        return self
     
    def groupByClause(self, clause):
        if self.dataverseName is None:
            raise Exception('No dataverse name set')

        if len(self.queryString) == 0:
            raise Exception("GROUP BY cann't start a query")
        else:
            self.queryString = self.queryString + " group by " + clause
        
        return self

    def returnClause(self, clause):
        if self.dataverseName is None:
            raise Exception('No dataverse name set')

        if len(self.queryString) == 0:
            raise Exception("GROUP BY cann't start a query")
        else:
            self.queryString = self.queryString + " return " + clause
        
        return self
     
    def getQueryString(self):
        return self.queryString

    def reset(self):
        self.queryString = ''

    def execute(self):
        if self.asterixBaseURL is None or self.asterixServicePoint is None:
            raise Exception('Query Manager is NOT setup well!!!')
        else:            
            if len(self.queryString) > 0:
                request_url = self.asterixBaseURL + "/" + self.asterixServicePoint    
                query = "use dataverse " + self.dataverseName + "; " + self.queryString + ";"    
                log.info('Executing... ', query)
                                
                response = requests.get(request_url, params={"query": query})
                
                # response = requests.get(request_url, params = {"query" : query, "mode": "asynchronous"})
                # response = requests.get(request_url +"/result", params = {"handle" : "\"handle\":\"[59, 0]\""})

                # print(response.url)
                # print(response.status_code)
                # print(response.text)
                
                return response.status_code, response.text    

    @tornado.gen.coroutine
    def executeQuery(self, dataverseName, queryStatment):
        request_url = self.asterixBaseURL + "/" + "query"    

        query = "use dataverse " + dataverseName + "; "
        query = query + queryStatment + ";"

        params = {'query': query}
        request_url = request_url + "?" + urllib.parse.urlencode(params)
        # response = requests.get(request_url, params = {"query": query, 'output': 'json'})

        log.info('Executing... ' + query)
        
        httpclient = tornado.httpclient.AsyncHTTPClient()
        try:
            request = tornado.httpclient.HTTPRequest(request_url, method='GET')
            response = yield httpclient.fetch(request)
            return response.code, str(response.body, encoding='utf-8')
        except tornado.httpclient.HTTPError as e:
            log.error('Error ' + str(e))
        except Exception as e:
            log.error('Error ' + str(e))

        return 500, 'Query failed: ' + query

    @tornado.gen.coroutine
    def executeUpdate(self, dataverseName, query):
        request_url = self.asterixBaseURL + "/" + "update"
        query = "use dataverse " + dataverseName + "; " + query + ";"
        params = {'statements': query}
        request_url = request_url + "?" + urllib.parse.urlencode(params)
        # response = requests.get(request_url, params = {"query": query, 'output': 'json'})

        httpclient = tornado.httpclient.AsyncHTTPClient()
        try:
            request = tornado.httpclient.HTTPRequest(request_url, method='GET')
            response = yield httpclient.fetch(request)
            return response.code, str(response.body, encoding='utf-8')
        except tornado.httpclient.HTTPError as e:
            log.error('Error ' + str(e))
        except Exception as e:
            log.error('Error ' + str(e))

        return 500, 'Query failed: ' + query

    @tornado.gen.coroutine
    def executeAQL(self, dataverseName, query):
        request_url = self.asterixBaseURL + "/" + "aql"
        query = "use dataverse " + dataverseName + "; " + query + ";"
        params = {'aql': query}
        request_url = request_url + "?" + urllib.parse.urlencode(params)

        # response = requests.get(request_url, params = {"aql": query, 'output': 'json'})

        print (request_url)

        httpclient = tornado.httpclient.AsyncHTTPClient()
        try:
            request = tornado.httpclient.HTTPRequest(request_url, method='GET')
            response = yield httpclient.fetch(request)
            return response.code, str(response.body, encoding='utf-8')
        except tornado.httpclient.HTTPError as e:
            log.error('Error ' + str(e))
        except Exception as e:
            log.error('Erorr ', str(e))

        return 500, 'Query failed:' + query

    @tornado.gen.coroutine
    def executeDDL(self, dataverseName, ddlStatement):
        request_url = self.asterixBaseURL + "/" + "ddl"    
        statement = "use dataverse " + dataverseName + "; " + ddlStatement + ";"
        log.info('Executing... ' + statement)

        params = {'ddl': ddlStatement}
        request_url = request_url + "?" + urllib.parse.urlencode(params)

        httpclient = tornado.httpclient.AsyncHTTPClient()
        try:
            request = tornado.httpclient.HTTPRequest(request_url, method='GET', headers={'Accept': 'application/json'})
            response = yield httpclient.fetch(request)
            return response.code, str(response.body, encoding='utf-8')
        except tornado.httpclient.HTTPError as e:
            log.error('Error ' + str(e))
        except Exception as e:
            log.error('Error ' + str(e))

        return 500, 'Query failed:' + ddlStatement
