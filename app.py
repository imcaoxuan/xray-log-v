import os
import time
from apscheduler.schedulers.background import BackgroundScheduler
from file_read_backwards import FileReadBackwards
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from log_util import parse_log

app = Flask(__name__)
DB_USER = os.environ['XRAY_LOG_V_DB_USER']
DB_PASS = os.environ['XRAY_LOG_V_DB_PASS']
DB_HOST = os.environ['XRAY_LOG_V_DB_HOST']
DB_PORT = os.environ['XRAY_LOG_V_DB_PORT']
DB_NAME = os.environ['XRAY_LOG_V_DB_NAME']
CRON_HOUR = os.environ.get('XRAY_LOG_V_CRON_HOUR', 12)
CRON_MIN = os.environ.get('XRAY_LOG_V_CRON_MIN', 0)

app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Access(db.Model):

    __tablename__ = 'access'

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    create_time = db.Column(
        db.DateTime,
        nullable=False,
        server_default=db.func.current_timestamp()
    )
    update_time = db.Column(
        db.DateTime,
        nullable=False,
        server_default=db.func.current_timestamp(),
        onupdate=db.func.current_timestamp()
    )
    date = db.Column(db.String(255), nullable=False)
    time = db.Column(db.String(255), nullable=False)
    address = db.Column(db.String(255), nullable=False)
    source_port = db.Column(db.String(255), nullable=False)
    protocol = db.Column(db.String(255), nullable=False)
    host = db.Column(db.String(255), nullable=False)
    target_port = db.Column(db.String(255), nullable=False)
    inbound = db.Column(db.String(255), nullable=False)
    outbound = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(255), nullable=False)
    remarks = db.Column(db.String(255), nullable=False)

    def __repr__(self):
        return f"<Access {self.id}>"


def dump2mysql():
    today = time.strftime('%Y/%m/%d')
    with app.app_context():
        print(f"dump2mysql started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        with FileReadBackwards('xray.log', encoding='utf-8') as frb:
            access_list = []
            for line in frb:
                access = parse_log(line)
                if access:
                    date_, time_, ip_address_, source_port_, protocol_, host_, target_port_, inbound_, outbound_, email_, reason_ = access
                    if date_ < today:
                        print('break')
                        break
                    access = Access(
                        date=date_,
                        time=time_,
                        address=ip_address_,
                        source_port=source_port_,
                        protocol=protocol_,
                        host=host_,
                        target_port=target_port_,
                        inbound=inbound_,
                        outbound=outbound_,
                        email=email_,
                        remarks=reason_
                    )
                    access_list.append(access)
                if len(access_list) == 100:
                    db.session.add_all(access_list)
                    db.session.commit()
                    print(f'add {len(access_list)} records to mysql')
                    access_list.clear()
            db.session.add_all(access_list)
            db.session.commit()
            print(f'add {len(access_list)} records to mysql')
            access_list.clear()
        print(f"dump2mysql ended at: {time.strftime('%Y-%m-%d %H:%M:%S')}")


scheduler = BackgroundScheduler()

scheduler.add_job(func=dump2mysql, trigger='cron', hour=CRON_HOUR, minute=CRON_MIN)

scheduler.start()


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
