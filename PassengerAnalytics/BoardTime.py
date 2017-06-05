from datetime import datetime
import MySQLdb

db_connection = MySQLdb.connect(host="localhost", user="IndigoProd", passwd="IndigoProd", db="IndigoProd")
cursor = db_connection.cursor()



def boardTime(mac,lDate,mvmnt):

	if mvmnt == "ICHQ001-ILad001":

		# for start time grab last time of ICHQ001

		sql="SELECT * FROM WifiLogs WHERE Mac = '" + str(mac) + "' AND LDate = '" +str(lDate) + "' AND DevId = 'ICHQ001' ORDER BY LTime DESC"
		print sql
		cursor.execute(sql)
		newSet = cursor.fetchall()
		newSetRows = cursor.rowcount
		if newSetRows > 0:
			sTime = newSet[0][7]

		# for end time grab first time of Lad0002

		sql="SELECT * FROM WifiLogs WHERE Mac = '" + str(mac) + "' AND LDate = '" +str(lDate) + "' AND DevId = 'ILad001' ORDER BY LTime"
		cursor.execute(sql)
		newSet = cursor.fetchall()
		newSetRows = cursor.rowcount
		if newSetRows > 0:
			eTime = newSet[0][7]


		mvmnt = "ICHQ001-ILad001"



	elif mvmnt == "ICHQ001-Lad0002":
		# for start time grab last time of ICHQ001

		sql="SELECT * FROM WifiLogs WHERE Mac = '" + str(mac) + "' AND LDate = '" +str(lDate) + "' AND DevId = 'ICHQ001' ORDER BY LTime DESC"
		print sql
		cursor.execute(sql)
		newSet = cursor.fetchall()
		newSetRows = cursor.rowcount
		if newSetRows > 0:
			sTime = newSet[0][7]

		# for end time grab first time of Lad0002

		sql="SELECT * FROM WifiLogs WHERE Mac = '" + str(mac) + "' AND LDate = '" +str(lDate) + "' AND DevId = 'Lad0002' ORDER BY LTime"
		print sql
		cursor.execute(sql)
		newSet = cursor.fetchall()
		newSetRows = cursor.rowcount
		if newSetRows > 0:
			eTime = newSet[0][7]

		mvmnt = "ICHQ001-Lad0002"

	return sTime , eTime , mvmnt




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
				mvmnt = row[4]

				rowNum += 1
				

				try:	
					print mac , lDate , mvmnt
					sTime,eTime,mvmnt = boardTime(mac,lDate,mvmnt)
					
					timeDiff = eTime - sTime



					timeDiff = timeDiff.total_seconds() / 3600
					print timeDiff

					mvmnt_type = "BOT"
					print mvmnt

					if timeDiff <= 2 and timeDiff > 0:
						sql = "INSERT INTO MvmntTracker (DevId , Mac , Type , LDate , STime , ETime, Mvmnt) VALUES ('" + str(devId) + "','" + str(mac) + "','" + str(mvmnt_type) +  "','" + str(lDate) + "','" + str(sTime) + "','" + str(eTime) + "','" +  str(mvmnt) + "' )"
						print sql
						cursor.execute(sql)
						db_connection.commit()
					else:
						print "Time difference is greater than 2 hr"

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
