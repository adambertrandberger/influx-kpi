from influxdb import InfluxDBClient
import math
import time
import sys

def has_database(client, name):
    for item in client.get_list_database():
        if item['name'] == name:
            return True

    return False

def setup_databases(client):
    if not has_database(client, 'kpis'):
        client.create_database('kpis')
    if not has_database(client, 'signals'):
        client.create_database('signals')

def sine_wave(sample_count, phase=0):
    ''' Generates a single period of a sine wave with the given resolution '''
    return list(map(lambda x: math.sin((x/math.pi) + phase), range(0, sample_count)))

def capture_one_period(name, sample_count):
    return Capture(name, sample_count) \
        .signal('Va') \
        .signal('Vb', math.pi/3) \
        .signal('Vc', (2*math.pi)/3) \
        .signal('Ia') \
        .signal('Ib', math.pi/2) \
        .signal('Ic', math.pi)

class Capture:
    ''' Emulates capturing sample_count many samples of signal data from an electrical device '''
    def __init__(self, name, sample_count, start_time=None, durationMs=1000):
        if not start_time:
            start_time = time.time_ns()

        self.name = name
        self.signals = {}
        self.signalConfig = {}
        self.start_time = start_time
        self.durationNs = durationMs * 10000 # how many milliseconds in duration is each capture
        self.sample_count = sample_count # how many samples are in each capture

    def signal(self, name, phase=0):
        ''' Adds a signal to the capture '''
        self.signalConfig[name] = phase
        return self

    def run(self):
        while True:
            self.signals = {}
            for name in self.signalConfig:
                self.signals[name] = sine_wave(self.sample_count, self.signalConfig[name])

            end_time = self.start_time + self.durationNs
            self.time = [x for x in range(self.start_time, end_time)]
            self.start_time = end_time
            
            yield self.to_influx()

    def to_influx(self):
        batches = []
        for i in range(self.sample_count):
            signals = []
            for name in self.signals:
                signals.append(self.signals[name][i])

            current_time = self.start_time
            if i > 0:
                current_time = self.start_time + math.floor(self.durationNs/(i / self.sample_count))


            batches.append({
                'signals': signals,
                'time': current_time
            })

        signal_names = list(self.signals.keys())
        
        influx = []
        for batch in batches:
            fields = []
            for i in range(len(self.signals)):
                fields.append(f'{signal_names[i]}={batch["signals"][i]}')
                
            influx.append(f'{self.name} {",".join(fields)} {batch["time"]}')

        return influx

def write_measurement(name):
    client = InfluxDBClient(host='localhost', port=8086)
    SAMPLE_COUNT = 20000
    setup_databases(client);
    
    capture = capture_one_period(name, SAMPLE_COUNT)

    for line_protocols in capture.run():
        client.write_points(line_protocols, database='signals', batch_size=SAMPLE_COUNT, protocol='line')
        print(f'Sent one {SAMPLE_COUNT} sized batch to InfluxDB')
        

    
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Requires a measurement name')
    else:
        write_measurement(sys.argv[1])
