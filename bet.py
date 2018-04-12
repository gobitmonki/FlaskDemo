import os,datetime,csv,MySQLdb

FILE = '66.csv'
FILE_DIR = './data/'
HOST = 'localhost'
PORT = 3306
USER = 'william'
PASSWD = 'escaton615'
DB = 'live'
BATCH_SIZE = 1000
DROP_STMT = 'drop table live'
CREATE_STMT = '''
create table live(
id bigint not null primary key auto_increment,
leagueID int,leagueOrder varchar(10),homeTeamID int,awayTeamID int,
matchOrder varchar(10),isNeutralGround int,matchStartTime varchar(40),
matchTimeHalf int,matchTimeMinute int,homeScore int,awayScore int,
homeRedCards int,awayRedCards int,siteID int,matchID int,FTAHPoddsID varchar(10),
FTAHP float,FTFavourite float,FTH float,FTA float,FTOUoddsID float,
FTOU float,FTO float,FTU float,FT1x2OddsID float,FT1x2H float,FT1x2A float,
FT1x2D float,FHAHPoddsID float,FHAHP float,FHFavourite float,
FHH float,FHA float,FHOUoddsID float,FHOU float,FHO float,FHU float,
FH1x2OddsID float,FH1x2H float,FH1x2A float,FH1x2D float,PlanNo int,
datatime varchar(40),market int);
'''
INSERT_STMT= '''
insert into live(
leagueID,leagueOrder,
homeTeamID,awayTeamID,matchOrder,isNeutralGround,matchStartTime,
matchTimeHalf,matchTimeMinute,homeScore,awayScore,homeRedCards,
awayRedCards,siteID,matchID,FTAHPoddsID,FTAHP,FTFavourite,FTH,FTA,
FTOUoddsID,FTOU,FTO,FTU,FT1x2OddsID,FT1x2H,FT1x2A,FT1x2D,
FHAHPoddsID,FHAHP,FHFavourite,FHH,FHA,FHOUoddsID,FHOU,FHO,FHU,
FH1x2OddsID,FH1x2H,FH1x2A,FH1x2D,PlanNo,datatime,market
)
values(
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s)
'''

class CSVReader():
    def __init__(self, path, size, handle):
        self.path = path
        self.files = os.listdir(path)
        self.size = size
        self.handle = handle

    def parser(self,row):
        for i in range(0,len(row)):
            elem = row[i].strip()
            if elem == 'NULL':
                elem = None
            if elem is not None and elem.startswith('.'):
                elem = "0" + elem
            row[i] = elem
        return row

    def read(self, filename, handle):
        print filename
        filename = self.path + filename
        size = self.size
        begin = datetime.datetime.now()
        count = 0
        batch = 0
        data = []
        with open(filename) as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) is not 44:
                    print "skip"
                    continue
                count = count + 1
                row = self.parser(row)
                if batch < size:
                    data.append(tuple(row))
                    batch = batch + 1
                else:
                    handle.run(INSERT_STMT,data)
                    data = []
                    batch = 0
            handle.run(INSERT_STMT,data)
        end = datetime.datetime.now()
        print "row count:" + str(count)
        print end - begin

class MysqlSource():
    def __init__(self, phost, pport, puser, ppasswd,pdb):
        print phost,pport,puser,ppasswd,pdb
        self.conn = MySQLdb.connect(
                host=phost, port=pport,user=puser,
                passwd=ppasswd,db=pdb)

    def run(self, sql, data):
        try:
            print len(data)
            cur = self.conn.cursor()
            cur.executemany(INSERT_STMT, data)
            self.conn.commit()
        except Exception,e:
            print e
        finally:
            cur.close()

    def close(self):
        self.conn.close()

def main():
    ds = MysqlSource(HOST,PORT,USER,PASSWD,DB)
    reader = CSVReader(FILE_DIR,BATCH_SIZE,ds)
    reader.read(FILE, ds)
    ds.close()

if __name__ == "__main__":
    main()
