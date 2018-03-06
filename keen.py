
import requests
import pprint
import csv
import sys
import json
import pymysql
import time

from datetime import datetime
import getpass
from pymongo import MongoClient

t0 = time.clock()

pp = pprint.PrettyPrinter(indent=4)

startDate = '2018-02-19'

endDate = '2018-03-04'


def keen_api():

	api_key = '88fc63c20731c2f09ea44ca264c3043d5403f78329a42a2b435690e94c39dff5d5ae487052e1d583c2dccf5b64cfc9add1c0cc6bb9a661cc91df4d0e545f8ad0ec1d831981907c44bc4ba7f1b062711aadec6e7549800042076726d73dadc454'

	api_url = 'https://api.keen.io/3.0/projects/56ddffe896773d7e98d63393/queries/count?api_key=' + api_key 

	data = {'event_collection': 'answerQuestion',
			'timezone': 'UTC',
			'group_by': ['request.siteUUID', 'questionUUID'],
			# 'latest': 1000,
			'timeframe': 'previous_14_days',
			# {'start': '2018-2-27T00:00:01.000Z', 'end': '2018-2-29T00:00:01.000Z'}
			# 'this_1_days',
			# 'property_names': ['questionUUID', 'questionOptionUUID', 'type', 'request.siteUUID'],
			# 'filter' : []
			}

	tokenHeaders = {'Content-Type': 'application/json'}

	response = requests.post(api_url, params = data, headers=tokenHeaders)
	print (response)

	keen = response.json()['result']

	return keen

def SQL():

	SQL_list = []

	count = 0
	for database in ["EMBED", "EVERTEST", "EVERTEST"]:
		# "EMBED", "EVERTEST", 
		if count == 0:
			SQL = "SELECT s.siteUUID, s.siteURL  \
					from SITE as s;"
		elif count == 1:
			SQL = "SELECT p.PredictionUUID, p.PredictionTitle \
				   from EVERTEST.PREDICTIONS as p;"
		else:
			SQL = "SELECT p.PredictionUUID as QuestionUUID, t.TVShowTitle as Category \
					from EVERTEST.PREDICTIONS as p 									\
					join EVERTEST.HASHTAGRELATIONSHIP as h ON p.PredictionUUID = h.PredictionUUID  \
					join EVERTEST.TVSHOW as t ON t.TVShowUUID = SUBSTR(h.HashTagText, 2)"

		db = pymysql.connect(host="prod-read-replica.cpbybmeoadzj.us-east-1.rds.amazonaws.com",    # your host, usually localhost
								user="readonly",         # your username
								passwd="rdslionking12",  # your password
								db=database)        # name of the data base

		cur = db.cursor()

		cur.execute(SQL)

		match_dict = {}
		for row in cur.fetchall():
			match_dict.update({row[0]:row[1]})

		SQL_list.append(match_dict)

		count += 1

	return SQL_list

siteUUID_sites = SQL()[0]
questionUUID_question = SQL()[1]
questionUUID_type = SQL()[2]



def formatPageViewsData(queryData):
	formatted_pageViewsData = {}
	for entry in queryData:
		siteUUID = entry['_id']['siteUUID']
		value = entry['embedPageViews']
		formatted_pageViewsData.update({siteUUID : value})
	return formatted_pageViewsData

###################################################### Options ###########################################################

def getPageViewsData():

	# Connect to production database
	client = MongoClient('ds015876-a0.mlab.com', 15876)
	db = client.analysis
	db.authenticate('analysist001', 'analysistrocks321', source='analysis')

	collections = client.analysis.revenuedata
	
	pipeline = [{'$match':{'date':{'$gte':startDate,'$lte': endDate}}},{'$group':{'_id':{'siteUUID':'$siteUUID'}, 'embedPageViews':{'$sum':'$embedPageViews'}}}]

	# Fire query to get data
	pageViewsData = collections.aggregate(pipeline)
	
	# Format query results
	formatted_pageViewsData = formatPageViewsData(pageViewsData)

	return formatted_pageViewsData

###################################################### Main call ###########################################################

pageview = getPageViewsData()
# print (getPageViewsData)



def data_combined():

	count = 0
	with open("C:\Users\Nick Liu\Desktop\\keen4.csv", "wb") as f:
		writer = csv.writer(f)

		for val in keen_api():
			# val['request'] = val['request']['siteUUID']
			# val['request.siteUUID'] = val.pop('request')

			siteUUID = val['request.siteUUID']
			questionUUID = val['questionUUID']
			click = val['result']
			if siteUUID in siteUUID_sites.keys():
				val.update({'site': siteUUID_sites[siteUUID]})
				val.update({'pageview': pageview[siteUUID]})
			else:
				val.update({'site': 'null'})
				val.update({'pageview': 0})
			pageview1 = val['pageview']
			val.update({'question': questionUUID_question[questionUUID]})
			if pageview1 > 0:
				val.update({'engagement_rate': float(click) / int(pageview1) * 10000000})
			else:
				val.update({'engagement_rate': 0})

			if questionUUID in questionUUID_type.keys():
				val.update({'question_type': questionUUID_type[questionUUID]})
			else:
				val.update({'question_type': 'null'})

			if count == 0:
				header = val.keys()
				writer.writerow(header)

				count += 1
			writer.writerow(val.values())
	print("Writing complete")

data_combined_pulled = data_combined()

# keen = keen_api()

# count = 0
# with open("C:\Users\Nick Liu\Desktop\\keen.csv", "wb") as f:
# 	writer = csv.writer(f)
# 	for val in keen:
# 		val['request'] = val['request']['siteUUID']
# 		val['request.siteUUID'] = val.pop('request')

# 		if count == 0:
# 			header = val.keys()
# 			writer.writerow(header)

# 			count += 1
# 		writer.writerow(val.values())
# print("Writing complete")

t1 = time.clock()
time_spend = t1 - t0
print (time_spend)