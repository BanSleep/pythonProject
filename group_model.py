from sqlalchemy.orm import sessionmaker
from marshmallow import Schema, fields
from sqlalchemy import create_engine, Column, Integer, String, null
from sqlalchemy.ext.declarative import declarative_base
import sqlite3
from datetime import datetime


Base = declarative_base()


def getModel(table_name):
    class GroupModel(Base):
        __tablename__ = table_name
        __table_args__ = {'extend_existing': True}
        id = Column(Integer, primary_key=True)
        startNumber = Column(Integer)
        fio = Column(String(32))
        dateBirth = Column(String(32))
        team = Column(String(32))
        startTime = Column(String(32))
        finishTime = Column(String(32))
        result = Column(String(32))
        gap = Column(String(32))
        place = Column(Integer)
        distance = Column(String(32))

    return GroupModel


class GroupSchema(Schema):
    id = fields.Integer(dump_only=True)
    startNumber = fields.Integer()
    fio = fields.String()
    dateBirth = fields.String()
    team = fields.String()
    startTime = fields.String()
    finishTime = fields.String()
    result = fields.String()
    gap = fields.String()
    place = fields.Integer()
    distance = fields.String()


def get_users_from_db(db_name, table_name):
    global serialized_users
    db_url = f'sqlite:///{db_name}'
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    if table_name != 'info':
        print(table_name)
        users = session.query(getModel(table_name)).all()
        user_schema = GroupSchema(many=True)
        serialized_users = user_schema.dump(users)

    return serialized_users


def finish_user(db_name, table_name, start_number, finish_time):
    db_url = f'sqlite:///databases/{db_name}'
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    user = getModel(table_name)
    users = session.query(user).filter(user.startNumber == start_number)
    user_schema = GroupSchema(many=True)
    serialized_users = user_schema.dump(users)
    all_users = user_schema.dump(session.query(getModel(table_name)).all())

    conn = sqlite3.connect(f'databases/{db_name}')
    cursor = conn.cursor()
    dateformat = '%H:%M:%S'
    date_obj = datetime.strptime(serialized_users[0]['startTime'], dateformat)
    print(finish_time)
    date_obj2 = datetime.strptime(finish_time, dateformat)
    result = str(date_obj2 - date_obj)
    gap = '0:00:00'
    if all_users[0]['result'] is not None:
        if datetime.strptime(result, dateformat) <= datetime.strptime(all_users[0]['result'], dateformat):
            for elm in all_users:
                if elm['result'] is not None and elm['startNumber'] != start_number:
                    date = datetime.strptime(elm['result'], dateformat)
                    cursor.execute(f"UPDATE {table_name} SET gap = ? WHERE startNumber = ?", (str(date - (datetime.strptime(result, dateformat))), elm['startNumber']))
        else:
            date = datetime.strptime(all_users[0]['result'], dateformat)
            gap = str((datetime.strptime(result, dateformat)) - date)
            cursor.execute(f"UPDATE {table_name} SET gap = ? WHERE startNumber = ?",
                           (str((datetime.strptime(result, dateformat)) - date), start_number))

    cursor.execute(f"UPDATE {table_name} SET result = ?, finishTime = ?, gap = ? WHERE startNumber = ?", (result, finish_time, gap, start_number))
    cursor.execute("""CREATE TABLE temp_table (
            id INTEGER PRIMARY KEY,
            startNumber INTEGER NOT NULL,
            fio TEXT NOT NULL,
            dateBirth TEXT NOT NULL,
            team TEXT NOT NULL,
            startTime TEXT NOT NULL,
            finishTime TEXT,
            result TEXT,
            gap TEXT,
            place INTEGER,
            distance TEXT NOT NULL,
            )""")
    cursor.execute(f"""INSERT INTO temp_table (startNumber, fio, dateBirth, team, startTime, finishTime,
        result, gap, place, distance) SELECT startNumber, fio, dateBirth, team, startTime, finishTime, result, gap, place, distance FROM
        {table_name} ORDER BY case when result is null then 1 else 0 end, result""")
    cursor.execute(f"""DROP TABLE {table_name}""")
    cursor.execute(f"""ALTER TABLE temp_table RENAME TO {table_name}""")
    conn.commit()
    cursor.close()
    conn.close()

    return serialized_users
