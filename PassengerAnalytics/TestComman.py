import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+mysqldb://IndigoProd:IndigoProd@localhost/IndigoProd")


def main():
    sql = "SELECT LDate, Mac, STime, ETime , Mvmnt FROM MvmntTracker"

    df = pd.read_sql(sql, engine)

    df_chopped = df.loc[df.duplicated(['Mac','Mvmnt'], keep=False)]

    unique_checkin_frame =  df_chopped[~df_chopped.duplicated(['Mac' , 'Mvmnt'], keep='first')]

    print unique_checkin_frame

    #
    # df_sliced = df[df.duplicated(['Mac'], keep=False)]
    #
    # print df_sliced


if __name__ == '__main__':
    main()
DA:f4:8b:32:21:4c:27

DA:98:0c:a5:d7:c4:35 
