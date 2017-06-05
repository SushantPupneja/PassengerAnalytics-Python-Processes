import pandas as pd
from sqlalchemy import create_engine
import datetime
import time

engine = create_engine("mysql+mysqldb://IndigoProd:IndigoProd@localhost/IndigoProd")



sql = "SELECT * FROM MvmntTracker"


try:
	# import mvmnttracker in df frame
	df = pd.read_sql(sql, engine)

except Exception as e:

	print "Pandas Connection Failed"
	raise e



def TrackMvmnt(mac):

	try:
		
		record = df.loc[(df["Mac"] == mac) & (df["Mvmnt"] != 'NA')]
		

	except Exception as e:

		print "TrackMvmnt in pandas failed"
		raise e
	

	if len(record) == 1:
		return 1 , record
	else:
		return 0 , 0


def main():

	panda_frame = pd.DataFrame({

		"DevId": [],
        "Mac": [],
        "LDate":[],
        "ITime":[],
        "LTime":[],
        "XTime":[],
        "Mvmnt":[],
        "Y":[],
        "Z":[],

		})

	sql = "SELECT * FROM MvmntTracker"


	try:

		# slice frame where with checkin conjestions  
		checkIn_Frame = df.loc[df["Mvmnt"] == 'NA']

		print len(checkIn_Frame)

	except Exception as e:
		print "Pandas Connection Failed"
		raise e



	for i in range(len(checkIn_Frame)-1):
		row = checkIn_Frame.loc[i]
		
		mac = str(row[2])
		
		if_mvmnt, record = TrackMvmnt(mac)

		if if_mvmnt == 1:

			DevId = str(row[1])
			LDate = str(row[4])
			ITime = row[5]
			LTime = row[6]

			checkin_duration = LTime - ITime

			x = time.strptime(str(checkin_duration)[7:],'%H:%M:%S')
			checkin_duration = datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds() / 60

			print checkin_duration
		
			IT = str(ITime)[7:]
			LT = str(LTime)[7:]



			Mvmnt = record.get_value(record.index[0],'Mvmnt') 
		
			XTime = str(record.get_value(record.index[0],'ETime'))


			boarding_duration = XTime - LTime

			y = time.strptime(str(boarding_duration)[7:],'%H:%M:%S')
			boarding_duration = datetime.timedelta(hours=y.tm_hour,minutes=y.tm_min,seconds=y.tm_sec).total_seconds() / 60



			t = XTime[7:]



			a_row = [DevId,IT,LDate,LT,mac,Mvmnt,t,checkin_duration,boarding_duration]
			panda_frame.loc[len(panda_frame)] = a_row

	panda_frame.to_csv(r'/home/ubuntu/Desktop/PassengerAnalytics/pandas.csv', header=None, index=None, mode='a', sep=' ')


if __name__ == '__main__':
	main()
