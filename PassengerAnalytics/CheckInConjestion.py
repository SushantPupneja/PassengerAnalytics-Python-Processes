from datetime import datetime
import MySQLdb

db_connection = MySQLdb.connect(host="localhost", user="IndigoProd", passwd="IndigoProd", db="IndigoProd")
cursor = db_connection.cursor()




def alreadyInserted(mac,lDate):


	sql="SELECT * FROM MvmntTracker WHERE Mac = '" + str(mac) + "' AND LDate = '" +str(lDate) + "'"


	try:

		cursor.execute(sql)

		if cursor.rowcount !=0:

			print "Mac already exists in table"
			return 1

		else:

			return 0


	except Exception as e:
		print "Database query failed"
		raise e


def trackConjestion(mac,devId,lDate):
	sql="SELECT * FROM WifiLogs WHERE Mac = '" + str(mac) + "' AND LDate = '" +str(lDate) + "' AND DevId = '" + str(devId) + "' ORDER BY LTime"
	cursor.execute(sql)
	newSet = cursor.fetchall()
	newSetRows = cursor.rowcount

	if newSetRows > 0:
		sTime = newSet[0][7]
		eTime = newSet[int(newSetRows)-1][7]
		return sTime , eTime


def main():


	try:

		sql = "SELECT * FROM CommonMac"
		cursor.execute(sql)
		resultSet = cursor.fetchall()
		numRows = cursor.rowcount
		print numRows
		rowNum = 0

	except Exception as e:

		print "Database query Failed"
		raise e




	while (True):
		

			if numRows > 0:

				row = resultSet[rowNum]
				devId = row[1]
				lDate = row[2]
				mac = row[3]

				rowNum += 1


				if devId == "ICHQ001":

					try:

						insertedFlag = alreadyInserted(mac,lDate)

						if insertedFlag == 0:


							sTime,eTime = trackConjestion(mac,devId,lDate)
							
							timeDiff = eTime - sTime

							timeDiff = timeDiff.total_seconds() / 3600
							print timeDiff

							mvmnt_type = "CWT"

							if timeDiff <= 1 and timeDiff > 1:
								sql = "INSERT INTO MvmntTracker (DevId , Mac , Type , LDate , STime , ETime) VALUES ('" + str(devId) + "','" + str(mac) + "','" + str(mvmnt_type) + "','" + str(lDate) + "','" + str(sTime) + "','" + str(eTime) + "')"
								print sql
								cursor.execute(sql)
								db_connection.commit()
							else:
								print "Time difference is greater than 1 hr"

					except Exception as e:
						print "Database Query Failed"
						db_connection.rollback()
						raise e

				


				if rowNum >= numRows:
					break






			else:
				print "No rows to process"


	db_connection.close()


if __name__ == "__main__":
	main()
