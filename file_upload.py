import json
import pandas as pd
import sqlalchemy
import argparse
import glob

#Command line argument using args parser
def myparser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_dir",required=True,help="file path of multiple csv files")
    parser.add_argument("--mysql_details",required=True,help="json connection details")
    parser.add_argument("--destination_table",required=True,help="which table do you want to generate")
    args = parser.parse_args()
    print("args:",args)
    var1 =vars(args)
    print("var1:",var1)
    return var1

#Database connection
class FileUploader:

    def create_db_engine(self,user,password,host,db):
        con1 = sqlalchemy.create_engine(
            'mysql+pymysql://{}:{}@{}/{}'.format(user, password, host, db))
        return con1

    def read_file(self,source):
        print("source:",source)
        all_files = glob.glob(source+"/*.csv")
        print('all_files:',all_files)
        li = []
        print(li)

        for filename in all_files:
            df = pd.read_csv(filename, index_col=None, header=0)
            li.append(df)

        frame = pd.concat(li, axis=0, ignore_index=True)
        print("len of list:", len(li))
        return frame

def main():
    var1 = myparser()

    # file path for csv source_dir
    file_path = var1['source_dir']

    # read file mysql_details.json  from local to get sql details
    with open(var1['mysql_details']) as f:
        sql_details = json.load(f)
    print("Sql_details:", sql_details)

    # Table name to be created in the database
    table_name = var1['destination_table']

    host = sql_details['host']
    user = sql_details['user']
    password = sql_details['password']
    db = sql_details['database']

    con1 = FileUploader()
    con2 = con1.create_db_engine(user, password, host, db)

    df = con1.read_file(file_path)

    try:
        df.to_sql(name=table_name, con=con2, if_exists='fail', index=False)
        print('Sucessfully written to Database!!!')

    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()

#command line arguments to pass
#python file_upload.py --source_dir G:\aws_project --mysql_details mysql_details.json --destination_table startup
