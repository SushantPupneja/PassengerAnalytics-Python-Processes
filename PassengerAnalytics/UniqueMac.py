import MySQLdb
import pandas as pd
from sqlalchemy import create_engine



db_connection = MySQLdb.connect(host='localhost', user='IndigoProd', passwd='IndigoProd', db='IndigoProd')
cursor = db_connection.cursor()	

# for pandas connection
# logs = pd.read_sql('SELECT * FROM WifiLogs', con=db_connection)

engine = create_engine("mysql+mysqldb://IndigoProd:IndigoProd@localhost/IndigoProd")

def main():
	#create a dataFrame where DevId = "ICHQ001"
	#sql = "SELECT * FROM WifiLogs WHERE TRIM(TRAILING 'dB'FROM Signaldb) > -55 and (LDate = '2017-05-12') and DevId = 'ICHQ001'"
	panda_frame = pd.DataFrame({

		"DevId": [],
        	"LDate": [],
        	"Mac":[]

		})

	try:
		sql = "SELECT * FROM WifiLogs"
		cursor.execute(sql)
		resultset = cursor.fetchall()
		numrows = cursor.rowcount
		rowcount = 0


	except Exception as e:
		print "Database query failed"
		raise e


	
	if numrows > 0:

		while (True):


			row = resultset[rowcount]
			devId = str(row[1])
			mac = str(row[2])
			date = str(row[6])


			if len(panda_frame) == 0:

				a_row = [devId,date,mac]  
				panda_frame.loc[len(panda_frame)] = a_row
				

			else:
				print mac 
				filtered_frame = panda_frame.loc[(panda_frame['Mac'] == mac) & (panda_frame['DevId'] == devId) & (panda_frame['LDate'] == date)]

				if len(filtered_frame) == 0:
					a_row = [devId,date,mac]  
					panda_frame.loc[len(panda_frame)] = a_row


				else:
					print "Row already exists"

			
			rowcount += 1

			
			if numrows <= rowcount:
				break


	#insert unique macs panda frame to the databse in Unique Mac on a given date Table 

		if panda_frame.count > 0:
			panda_frame.to_sql('UniqueMac', engine ,if_exists='append', chunksize=100, index=False, index_label=None)
	else:
		print "No more Records to process..."



if __name__ == "__main__":
	main()



