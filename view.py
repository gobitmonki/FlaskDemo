# -*- coding=UTF-8 -*-
from flask import Flask, render_template
from flask import request
from flask import jsonify
from flask_cors import CORS
import os, MySQLdb

app = Flask(__name__)
app.config.from_object(__name__)
CORS(app)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/getSettings', methods=['GET','POST'])
def getSettings():
    return "hello world"

@app.route('/getSubscribe', methods=['GET','POST'])
def getSubScribed():
    rs=[]
    conn = getConn()
    cursor = conn.cursor()
    stmt = "select * from subscribe_match_tbl where status <>'Done' and status <> 'remove'"
    n = cursor.execute(stmt)
    for row in cursor.fetchall():
        r = {}
        matchID = row[0]
        leagueID = row[1]
        homeTeamID = row[2]
        awayTeamID = row[3]
        matchTime = row[4]
        status = row[5]
        rs.append({
            "matchID": row[0],
            "leagueID": row[1],
            "homeTeamID": row[2],
            "awayTeamID": row[3],
            "matchTime": row[4],
            "status": row[5]
            })
    cursor.close()
    conn.close()
    return jsonify(rs)

@app.route('/addSubscribe/', methods=['GET','POST'])
def addSubscribe():
    leagueID = request.args.get("leagueID")
    homeTeamID = request.args.get("homeTeamID")
    awayTeamID = request.args.get("awayTeamID")
    matchTime = '2017-09-20 23:45:00'
    status = 'Not start'
    conn = getConn()
    cursor = conn.cursor()
    stmt = "insert into subscribe_match_tbl(leagueID,homeTeamID,awayTeamID,matchTime,status) values('%s','%s','%s','%s','%s')" % (leagueID,homeTeamID,awayTeamID,matchTime,status)
    n = cursor.execute(stmt)
    cursor.close()
    conn.commit()
    conn.close()
    return 'true'

@app.route('/removeSubscribe/', methods=['GET','POST'])
def removeSubscribe():
    leagueID = request.args.get("leagueID")
    homeTeamID = request.args.get("homeTeamID")
    awayTeamID = request.args.get("awayTeamID")
    conn = getConn()
    cursor = conn.cursor()
    stmt = "update subscribe_match_tbl set status='remove' where leagueid=%s and hometeamid=%s and awayteamid=%s;" % (leagueID,homeTeamID,awayTeamID)  
    n = cursor.execute(stmt)
    cursor.close()
    conn.commit()
    conn.close()
    return 'true'

@app.route('/chart/', methods=['GET','POST'])
def getChartInfo():
    rs = []
    conn = getConn()
    cursor = conn.cursor()
    leagueID = request.args.get("leagueID")
    homeTeamID = request.args.get("homeTeamID")
    awayTeamID = request.args.get("awayTeamID")
    stmt = "select leagueID,homeTeamID,awayTeamID,FTA,datatime from live where leagueID=%s and homeTeamID = %s and awayTeamID=%s limit 5;" %(leagueID, homeTeamID, awayTeamID)
    n = cursor.execute(stmt)
    for row in cursor.fetchall():
        rs.append({
            "name": row[4],
            "value": row[3]
            })
    cursor.close()
    conn.close()
    return jsonify(rs)

@app.route('/live/', methods=['GET','POST'])
def getLiveInfo():
    rs = []
    conn = getConn()
    cursor = conn.cursor()
    matchID = request.args.get("matchID")
    leagueID, homeTeamID, awayTeamID = matchID.split('-')
    stmt = "select leagueID,homeTeamID,awayTeamID,matchStartTime,homeScore,awayScore,homeRedCards,awayRedCards,FTA,FHA,datatime from live where leagueID=%s and homeTeamID = %s and awayTeamID=%s ORDER BY RAND() limit 20;" % (leagueID, homeTeamID, awayTeamID)
    print stmt
    n = cursor.execute(stmt)
    for row in cursor.fetchall():
        rs.append({
            "leagueID": row[0],
            "homeTeamID": row[1],
            "awayTeamID": row[2],
            "matchStartTime": row[3],
            "homeScore": row[4],
            "awayScore": row[5],
            "homeRedCards": row[6],
            "awayRedCards": row[7],
            "FTA": row[8],
            "FHA": row[9],
            "datatime": row[10]
            })
    cursor.close()
    return jsonify(rs)

@app.route('/ticket/', methods=['GET','POST'])
def getTicketInfo():
    rs = []
    conn = getConn()
    cursor = conn.cursor()
    matchID = request.args.get("matchID")
    leagueID, homeTeamID, awayTeamID = matchID.split('-')
    stmt = "select leagueID,homeTeamID,awayTeamID,homeScore,awayScore,datatime,bettype,ft,odds1,odds2,oddsformat,betside,payoff,pnl from ticket where leagueID=%s and homeTeamID = %s and awayTeamID=%s;" % (leagueID, homeTeamID, awayTeamID)
    print stmt
    n = cursor.execute(stmt)
    for row in cursor.fetchall():
        rs.append({
            "leagueID": row[0],
            "homeTeamID": row[1],
            "awayTeamID": row[2],
            "homeScore": row[3],
            "awayScore": row[4],
            "datatime": row[5],
            "bettype": row[6],
            "ft": row[7],
            "odds1": row[8],
            "odds2": row[9],
            "oddsformat": row[10],
            "betside": row[11],
            "payoff": row[12],
            "pnl": row[13]
            })
    cursor.close()
    return jsonify(rs)


def toJson(line):
    columns = ['leagueID','leagueOrder','homeTeamID','awayTeamID','matchOrder','isNeutralGround','matchStartTime','matchTimeHalf','matchTimeMinute','homeScore','awayScore','homeRedCards','awayRedCards','siteID','matchID','FTAHPoddsID','FTAHP','FTFavourite','FTH','FTA','FTOUoddsID','FTOU','FTO','FTU','FT1x2OddsID','FT1x2H','FT1x2A','FT1x2D','FHAHPoddsID','FHAHP','FHFavourite','FHH','FHA','FHOUoddsID','FHOU','FHO','FHU','FH1x2OddsID','FH1x2H','FH1x2A','FH1x2D','PlanNo','datatime','market']
    values = line.split(',')
    length = len(columns)
    i = 0
    rs = {}
    while i < length:
        rs[columns[i]] = values[i].strip()
        i = i + 1
    return rs
   
def getConn():
    host = 'localhost'
    port = 3306
    user = 'william'
    passwd = 'escaton615'
    db = 'live'
    conn = MySQLdb.connect(host=host,port=port,user=user,passwd=passwd,db=db)
    return conn

if __name__ == '__main__':
    app.run(host='0.0.0.0')
