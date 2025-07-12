import re

from file_read_backwards import FileReadBackwards

pattern = r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6}) from (\d+\.\d+\.\d+\.\d+):(\d+) (accepted|rejected) (\S+):(\S+):(\d+) \[(\S+) -> (\S+)\] email: (\S+)"


# 拆分protocol:host:port
def parse_log(log_entry):
    match = re.search(pattern, log_entry)
    if match:
        # 提取各个分组
        date, time = match.group(1).split(' ')
        ip_address = match.group(2) or ''
        source_port = match.group(3) or ''
        status = match.group(4) or ''
        protocol = match.group(5) or '' # 协议和主机端口部分
        host = match.group(6) or ''
        target_port = match.group(7) or ''
        inbound = match.group(8) or ''
        outbound = match.group(9) or ''
        email = match.group(10) or ''


        print(f"date time: {date} {time}")
        print(f"IP Address: {ip_address}")
        print(f"Port: {source_port}")
        print(f"Status: {status}")
        print(f"Protocol: {protocol}")
        print(f"Host: {host}")
        print(f"Target Port: {target_port}")
        print(f"Inbound: {inbound}")
        print(f"Outbound: {outbound}")
        print(f"Email: {email if email else ''}")
        return date, time, ip_address, source_port, protocol, host, target_port, inbound, outbound, email, ''
    else:
        print("No match found.")
        return None

# with FileReadBackwards('xray.log', encoding='utf-8') as frb:
#     for line in frb:
#         parse_log(line)
