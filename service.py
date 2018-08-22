
import pypyodbc 
import pandas as pd
import json
import psycopg2
from flask import Flask
from flask import Flask, jsonify
from flask import request



app = Flask(__name__)

cnxn = pypyodbc.connect("Driver={ODBC Driver 13 for SQL Server};"
                        "Server=localhost;"
                        "Database=InpatrimoniumNET_DEMO;"
                        "uid=SA;pwd=P@55w0rd")

@app.route("/api/record")
def record_by_id():
	id = request.args.get('id')

	df = pd.read_sql_query('select * from [dbo].[vw_Objecto_DublinCore] WHERE objecto_id = ' + str(id), cnxn)

	out = df.to_json(orient='records')[1:-1].replace('},{', '} {')

	print(out)
	return(out)


@app.route("/api/records")
def records():
	total_records = request.args.get('total_records')

	if int(total_records) <= 0:
		total_records = 100

	df = pd.read_sql_query('SELECT TOP ' + total_records + ' Objecto_ID AS ID FROM vw_Objecto_DublinCore', cnxn)


	out = df.to_json(orient='records')[1:-1].replace('},{', '} {')

	return(out)

@app.route("/api/records_by_date")
def records_by_date():
	initial_date = request.args.get('initial_date')
	final_date = request.args.get('final_date')
	
	sql = 'select * '
	sql = sql + 'from vw_Objecto_DublinCore AS Objectos '
	sql = sql + 'WHERE dt_Registo BETWEEN \'' + initial_date + '\' AND \'' + final_date + '\''
	
	df = pd.read_sql_query(sql, cnxn)


	out = df.to_json(orient='records')[1:-1]

	return(out)

@app.route("/api/delete_records")
def delete_records():
	initial_date = request.args.get('initial_date')
	final_date = request.args.get('final_date')
	

	sql = "EXECUTE sp_GetDeletedRecords '" + initial_date + "', '" + final_date + "'"
	
	cursor = cnxn.cursor()

	cursor.execute(sql)
	rowcount = cursor.rowcount
	cnxn.commit()

	print(rowcount)
	return("command 'delete_records' was executed.")

@app.route("/api/cronology")
def cronology():
	cron = request.args.get('cron')

	df = pd.read_sql_query('select * from [dbo].[vw_Objecto_DublinCore] where cronologia like ' + str(cron), cnxn)

	out = df.to_json(orient='records')[1:-1].replace('},{', '} {')

	return(out)

@app.route("/api/edited_records")
def edited_records():
	initial_date = request.args.get('initial_date')
	final_date = request.args.get('final_date')

	sql = 'EXECUTE sp_GetEditedRecords \'' + initial_date + "', '" + final_date + '\''
	
	df = pd.read_sql_query(sql, cnxn)

	out = df.to_json(orient='records')[1:-1].replace('},{', '} {')

	return(out)

@app.route("/api/url")
def url():
	url_id = request.args.get('url_id')

	in_web = "https://museudigital.marinha.pt/pesquisa/"
	in_web = in_web + "ficha.aspx?t=o&id=" + str(url_id)

	return(in_web)

if __name__ == '__main__':
    app.run(host="localhost",port=5000)

