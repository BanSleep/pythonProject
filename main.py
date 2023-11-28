import sqlite3
# import json
import config
from config import basedir
import table_converter
from datetime import datetime
# from group_model import get_group_model, group_schema, groups_schema
from flask import render_template, request
from sqlalchemy import create_engine, Column, Integer, String
from group_model import get_users_from_db, finish_user
import pathlib

from flask import jsonify

app = config.app


@app.route("/get-all")
def get_all():
    conn = sqlite3.connect(f'databases/{request.args["table_name"]}.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    all_sportsmans = list()
    mapa = {'groups': []}

    for name in tables:
        print(name[0] == 'info')
        if name[0] != 'info':
            data = get_users_from_db(db_name=f'databases/{request.args["table_name"]}.db', table_name=name[0])
            all_sportsmans.append(data)
            mapa['groups'].append({'groupName': name[0], "data": data})
    print(mapa)
    return mapa


@app.route('/create-table', methods = ['POST'])
def create_table():
    file = request.files['protocol']
    print(file.filename)
    file.save(f"protocols/{file.filename}.xlsx")
    table_converter.read_table(request.form.get('table_name'), file.filename, request.form.get('date'))
    return 'Success'


@app.route('/get-table-names')
def get_table_names():
    conn = sqlite3.connect(f'databases/{request.args.get("table_name")}.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    table_names = list()

    for elm in tables:
        table_names.append(elm[0])
    if 'info' in table_names:
        table_names.remove('info')
    return table_names


@app.route('/get-group')
def get_group():
    print(1)
    # group = get_group_model(suffix=request.args.get('group_name')).query.all()
    # return groups_schema.dump(group)


@app.route('/get-sportsman')
def get_sportsman():
    global group
    conn = sqlite3.connect('databases/Startovy_protokol_25_12_2022.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    # for name in tables:
    #     group = get_group_model(suffix=name[0]).query.filter(
    #         get_group_model(suffix=name[0]).startNumber == request.args.get('start_number')).one_or_none()
    #     if group:
    #         return group_schema.dump(group)

    return 'Not Found'


@app.route('/finish', methods=['POST'])
def finish():
    conn = sqlite3.connect(f'databases/{request.form.get("tableName")}.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    global table_name
    all_sportsmans = list()
    for name in tables:
        data = get_users_from_db(db_name=f'databases/{request.form.get("tableName")}.db', table_name=name[0])
        all_sportsmans.append(data)
        for user in data:
            if str(user['startNumber']) == str(request.form.get('startNumber')):
                table_name = name
    finish_user(db_name=f'{request.form.get("tableName")}.db', table_name=table_name[0],
                start_number=request.form.get('startNumber'), finish_time=request.form.get('finishTime'))
    cursor.close()
    conn.close()
    return 'Success!'


@app.route('/get-list-tournaments')
def get_list_tournaments():
    tables = pathlib.Path('databases')
    ready_model = {'result': []}
    for item in tables.iterdir():
        conn = sqlite3.connect(item)
        cursor = conn.cursor()
        cursor.execute('SELECT date FROM info')
        info = cursor.fetchall()[0]
        ready_model['result'].append({'name': str(item).replace('databases/', '').replace('.db', ''), "date": info[0]})
    return ready_model



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)

