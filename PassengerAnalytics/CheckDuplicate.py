import MySQLdb
import datetime
from datetime import datetime, timedelta 

db = MySQLdb.connect(host="localhost", user="IndigoProd", passwd="IndigoProd", db="IndigoProd")
cursor = db.cursor()


def time_tango(date, time):
	return datetime.strptime("{}, {}".format(date, time), "%Y-%m-%d, %H:%M:%S")



def checkDuplicate(mac,ldate):
	try:
		sql = "SELECT * FROM DuplicateLogs WHERE Mac IN ('" + str(mac) + "') and LDate = '" + str(ldate) + "'"

		cursor.execute(sql) 

		if int(cursor.rowcount) > 0:
			print "Mac " + mac + " already exists in DuplicateLogs on " + str(ldate) 
			return 0
		else:
			return 1
	except Exception as e:
		print "Database query failed"
		raise e
	
	


def main():
	cursor.execute("SELECT * FROM UniqueMac WHERE LDate = '2017-05-12'")
	resultset = cursor.fetchall()
	numrows = int(cursor.rowcount)
	print numrows
	print type(resultset)
	rowcount = 0
	activity_runnig = False


	# for i in range(0,numrows):
	while(True):

		
		if activity_runnig:
			
			try:

				sql = "SELECT * FROM UniqueMac WHERE Mac IN ('" + str(mac) + "') and LDate BETWEEN '" + str(three_days_from_time) + "' AND '" + str(two_hours_from_time) + "' ORDER BY LDate DESC"
				print sql
				cursor.execute(sql)


				if int(cursor.rowcount) > 0: 
					records = cursor.fetchall()
					three_day_streak = []
					
					
					for row in records:
						new_date = row[2]

						if new_date not in three_day_streak:
							three_day_streak.append(new_date)

					print three_day_streak


					
					if len(three_day_streak) >= 3:
						try:
							sql = "INSERT INTO DuplicateLogs (DevId, Mac, LDate, Status) VALUES ('" + str(devId) + "' ," + "'" + str(mac) + "' ," + "'" + str(date) + "' ," + " 1 );" 
							print sql
							cursor.execute(sql)
							db.commit()
							print "ALERT!!!!!!!!!!!!! DUPLICATE MAC FOUND"
							activity_runnig = False
						except Exception as e:
							db.rollback()
							print "Database query failed"
							activity_runnig = False
							raise e
						
					else:
						print "mac not in three days streak " + mac +  " " + str(date)
						activity_runnig = False
						
				else:
					activity_runnig = False
					print "activity completed"
			except IndexError:
					print "End of rows"
					break
		else:
			if len(resultset) > 0:
				row = resultset[rowcount]
				#logId = row[0]
				mac = row[3]
				#time = row[7]
				time = datetime.now().strftime("%H:%M:%S")
				date = row[2]
				date_time = time_tango(date,time)	
				#check for mac in duplicatelogs table
				checkFlag = checkDuplicate(mac,date)
				# try:
				# 	sql = "UPDATE WifiLogs SET Status = 1 WHERE LogId IN ('" + str(logId) + "')"
				# 	print sql
				# 	cursor.execute(sql)
				# 	db.commit()
				# except Exception as e:
				# 	print "Database query failed"
				# 	db.rollback()
				# 	raise e
				rowcount += 1

				if checkFlag == 1:
					#signalDb = row[3]
					devId = row[1]
					two_hours_from_time = date_time - timedelta(hours=2)
					three_days_from_time = date_time - timedelta(hours=48)

					two_hours_from_time = two_hours_from_time.strftime("%Y-%m-%d")
					three_days_from_time = three_days_from_time.strftime("%Y-%m-%d")
					print two_hours_from_time
					print three_days_from_time
					activity_runnig = True
					print "activity starts for mac " + mac
			else:
				print "No new Logs to process"

#Duplicate table query:

# CREATE TABLE DuplicateLogs (LogId int(11) auto_increment PRIMARY KEY, DevId VARCHAR(20), Mac VARCHAR(25), SignalDb VARCHAR(10), Type VARCHAR(10), LogDate datetime, LDate date, LTime time, SLogDate DATETIME DEFAULT CURRENT_TIMESTAMP, Status int(1));	

	#db.close()



if __name__ == "__main__":
	main()

