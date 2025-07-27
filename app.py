import calendar
import os
import ssl
import time
from datetime import date, datetime
from datetime import timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from file_read_backwards import FileReadBackwards
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy

from log_util import parse_log

app = Flask(__name__)
DB_USER = os.environ['XRAY_LOG_V_DB_USER']
DB_PASS = os.environ['XRAY_LOG_V_DB_PASS']
DB_HOST = os.environ['XRAY_LOG_V_DB_HOST']
DB_PORT = os.environ['XRAY_LOG_V_DB_PORT']
DB_NAME = os.environ['XRAY_LOG_V_DB_NAME']
MYSQL_SSL_CA = os.environ.get('XRAY_LOG_V_DB_CA', './ca.pem')
MYSQL_SSL_CERT = os.environ.get('XRAY_LOG_V_DB_CERT', './client-cert.pem')
MYSQL_SSL_KEY = os.environ.get('XRAY_LOG_V_DB_KEY', './client-key.pem')
CRON_HOUR = os.environ.get('XRAY_LOG_V_CRON_HOUR', 12)
CRON_MIN = os.environ.get('XRAY_LOG_V_CRON_MIN', 0)
XRAY_ACCESS_LOG = os.environ.get('XRAY_LOG_V_ACCESS_LOG', '/var/log/xray/access.log')
BATCH_SIZE = int(os.environ.get('XRAY_LOG_V_BATCH_SIZE', 100))

print(f"DB_USER: {DB_USER}")
print(f"DB_PASS: {DB_PASS}")
print(f"DB_HOST: {DB_HOST}")
print(f"DB_PORT: {DB_PORT}")
print(f"DB_NAME: {DB_NAME}")
print(f"MYSQL_SSL_CA: {MYSQL_SSL_CA}")
print(f"MYSQL_SSL_CERT: {MYSQL_SSL_CERT}")
print(f"MYSQL_SSL_KEY: {MYSQL_SSL_KEY}")
print(f"CRON_HOUR: {CRON_HOUR}")
print(f"CRON_MIN: {CRON_MIN}")
print(f"XRAY_ACCESS_LOG: {XRAY_ACCESS_LOG}")
print(f"BATCH_SIZE: {BATCH_SIZE}")

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_REQUIRED
ssl_context.load_verify_locations(cafile=MYSQL_SSL_CA)

ssl_args = {
    "ssl": ssl_context
}
app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'connect_args': ssl_args
}
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


def dump2mysql(from_datetime=datetime.now() - timedelta(days=1), to_datetime=datetime.now() - timedelta(days=1), log_file=None):
    # yesterday = datetime.now() - timedelta(days=1)
    # yesterday_str = yesterday.strftime('%Y/%m/%d')
    from_ = from_datetime.strftime('%Y/%m/%d')
    to_ = to_datetime.strftime('%Y/%m/%d')
    print(f"dump for: {from_}~{to_}")
    with app.app_context():
        print(f"dump2mysql started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        with FileReadBackwards(log_file or XRAY_ACCESS_LOG, encoding='utf-8') as frb:
            access_list = []
            for line in frb:
                try:
                    access = parse_log(line)
                    if access:
                        date_, time_, ip_address_, source_port_, protocol_, host_, target_port_, inbound_, outbound_, email_, reason_ = access
                        if date_ > to_:
                            continue
                        if date_ < from_:
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
                    if len(access_list) >= BATCH_SIZE:
                        print(f'add {len(access_list)} records to mysql')
                        db.session.add_all(access_list)
                        db.session.commit()
                        access_list.clear()
                except Exception as e:
                    print(e)
            print(f'add last {len(access_list)} records to mysql')
            db.session.add_all(access_list)
            db.session.commit()
            access_list.clear()
            db.session.close_all()
        print(f"dump2mysql ended at: {time.strftime('%Y-%m-%d %H:%M:%S')}")


scheduler = BackgroundScheduler()

scheduler.add_job(func=dump2mysql, trigger='cron', hour=CRON_HOUR, minute=CRON_MIN)

scheduler.start()


@app.route('/')
def hello_world():  # put application's code here
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


@app.post('/dump')
def dump():
    request_body = request.json
    from_datetime = datetime.fromtimestamp(request_body.get('from', calendar.timegm(date.today().timetuple())))
    to_datetime = datetime.fromtimestamp(request_body.get('to', time.time()))
    log_file = request_body.get('log_file', None)
    dump2mysql(from_datetime=from_datetime, to_datetime=to_datetime, log_file=log_file)
    return {'message': f'dump {from_datetime}~{to_datetime}' + log_file if log_file else ''}


if __name__ == '__main__':
    app.run()
