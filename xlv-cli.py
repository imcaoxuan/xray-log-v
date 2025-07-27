import argparse
import json
import time
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument('-l', '--log_file', type=str)
parser.add_argument('-f', '--from_timestamp', type=int, default=0)
parser.add_argument('-t', '--to_timestamp', type=int, default=int(time.time()))

if __name__ == '__main__':
    args = parser.parse_args()
    log_file = args.log_file
    from_timestamp = args.from_timestamp
    to_timestamp = args.to_timestamp
    print(from_timestamp)
    print(to_timestamp)
    print(log_file)
    data = {
        'from': from_timestamp,
        'to': to_timestamp,
        'log_file': log_file
    }
    encoded_data = json.dumps(data).encode('utf-8')
    request = urllib.request.Request('http://127.0.0.1:5000/dump',
                                     data=encoded_data,
                                     headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=3600) as response:
        content = response.read().decode('utf-8')
        print(json.loads(content))
