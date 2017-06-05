import MySQLdb
import pandas as pd
from sqlalchemy import create_engine



db_connection = MySQLdb.connect(host='localhost', user='IndigoProd', passwd='IndigoProd', db='IndigoProd')
cursor = db_connection.cursor()

# for pandas connection
# logs = pd.read_sql('SELECT * FROM WifiLogs', con=db_connection)

engine = create_engine("mysql+mysqldb://IndigoProd:IndigoProd@localhost/IndigoProd")


def checkDuplicate(mac,ldate):
        try:
            print mac
            sql = "SELECT * FROM DuplicateLogs WHERE Mac = '" + str(mac) + "'"

            cursor.execute(sql)
            print (cursor.fetchall())

            if int(cursor.rowcount) > 0:

                print "Mac " + mac + " exists in DuplicateLogs"

                return 0
            else:
                return 1

        except Exception as e:
            print "Database query failed"
            raise e





def main():
    #create a dataFrame where DevId = "ICHQ001"

    panda_frame = pd.DataFrame({

        "DevId": [],
            "LDate": [],
            "Mac":[]

        })

    try:
        sql = "SELECT * FROM UniqueMac"
        cursor.execute(sql)
        resultset = cursor.fetchall()
        numrows = cursor.rowcount
        print numrows
        rowcount = 0


    except Exception as e:
        print "Database query failed"
        raise e



    if numrows > 0:

        while (True):


            row = resultset[rowcount]
            devId = str(row[1])
            date = str(row[2])
            mac = str(row[3])


            checkFlag = checkDuplicate(mac,date)
            rowcount += 1


            if checkFlag == 1:
                    a_row = [devId,date,mac]
                    panda_frame.loc[len(panda_frame)] = a_row


            if numrows <= rowcount:
                print "End of rows"
                break


        #print panda_frame

    # slicing on the basis of DEVID


        ICHQ_frame = panda_frame.loc[panda_frame['DevId'] == 'ICHQ001']
        ILad001_frame = panda_frame.loc[panda_frame['DevId'] == 'ILad001']
        Lad0002_frame = panda_frame.loc[panda_frame['DevId'] == 'Lad0002']

        print "Total Number of Check In's " + str(len(ICHQ_frame))
        print "Total Number of passenger's on ILad001 " + str(len(ILad001_frame))
        print "Total Number of passenger's on Lad0002 " + str(len(Lad0002_frame))


    # # merge Frames of DevID ICHQ001 and ILad001
        combined_frame_1 = ICHQ_frame.append(ILad001_frame, ignore_index=True)


    # # Drop Duplicate values and others from combined_frame_1 keeping last
    # 	print "Table common mac in ICHQ001 and ILad001"
        common_frame_1 = combined_frame_1[combined_frame_1.duplicated(['Mac','LDate'], keep=False)]

        common_frame_a = common_frame_1[~common_frame_1.duplicated(['Mac','LDate'], keep='first')]

        if common_frame_a.count > 0:
            common_frame_a['Mvmnt'] = 'ICHQ001-ILad001'
            #common_frame_a.add('Mvmnt', axis='columns', level=None, fill_value='ICHQ001-ILad001')
            common_frame_a.to_sql('CommonMac', engine ,if_exists='append', chunksize=100, index=False, index_label=None)

    #  merge Frames of DevID ICHQ001 and Lad0002
        combined_frame_2 = ICHQ_frame.append(Lad0002_frame, ignore_index=True)

    # 	print "Table common mac in ICHQ001 and Lad0002"
    #  Drop Duplicate values and others from combined_frame_2 keeping last
        common_frame_2 = combined_frame_2[combined_frame_2.duplicated(['Mac','LDate'], keep=False)]

        common_frame_b = common_frame_2[~common_frame_2.duplicated(['Mac','LDate'], keep='first')]
        #print combined_frame_2

        if common_frame_b.count > 0:
            common_frame_b['Mvmnt'] = 'ICHQ001-Lad0002'
            #common_frame_b.add('Mvmnt', axis='columns', level=None, fill_value='ICHQ001-Lad0002')
            common_frame_b.to_sql('CommonMac', engine ,if_exists='append', chunksize=100, index=False, index_label=None)

        print "Comman Macs added in the table"
    # adding unique columns
        #comman_mac_frame = combined_frame_1.append(combined_frame_2, ignore_index=True)



if __name__ == "__main__":
    main()

