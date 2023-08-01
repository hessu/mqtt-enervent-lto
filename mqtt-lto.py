#!/usr/bin/python3

import logging
from logging.handlers import TimedRotatingFileHandler
import signal
import time
import os
import sys
import requests

import argparse
import yaml
import paho.mqtt.client as mqtt

import mqtt_graphite

from pyModbusTCP.client import ModbusClient

CONFIGFILE = "mqtt-lto.yaml"

# parse command line
parser = argparse.ArgumentParser()
parser.add_argument("--config", metavar="config", default=CONFIGFILE, help="YAML configuration file")
parser.add_argument("--debug", action='store_true', help="enable verbose logging")
args = parser.parse_args()

class Loader(yaml.SafeLoader):
    def __init__(self, stream):
        self._root = os.path.split(stream.name)[0]
        super(Loader, self).__init__(stream)

    def include(self, node):
        filename = os.path.join(self._root, self.construct_scalar(node))
        with open(filename, 'r') as f:
            return yaml.load(f, Loader)

Loader.add_constructor('!include', Loader.include)

with open(args.config, 'r') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=Loader)

DEBUG = cfg.get("debug") or args.debug
LOGFILE = cfg.get("logfile")

APPNAME = "mqtt-lto"

LOGFORMAT = '%(asctime)-15s %(message)s'
log_formatter = logging.Formatter(LOGFORMAT)

log = logging.getLogger(APPNAME)
if DEBUG:
    log.setLevel(logging.DEBUG)
else:
    log.setLevel(logging.INFO)
log_handler = TimedRotatingFileHandler(LOGFILE,
                                       when="w0",
                                       interval=1,
                                       backupCount=5)
log_handler.setFormatter(log_formatter)
log.addHandler(log_handler)


TX_INTERVAL = 30
GRAPHITE_PREFIX = "lto."

log.info("Starting " + APPNAME)
log.info("INFO MODE")
log.debug("DEBUG MODE")

def cleanup(signum, frame):
    """
    Signal handler to ensure we disconnect cleanly
    in the event of a SIGTERM or SIGINT.
    """
    log.info("Exiting on signal %d", signum)
    sys.exit(signum)


class LTOReg(object):
    def __init__(self, addr, name, *, signed=False, multiplier=None, round=None, test=False, bitmap=None):
        self.addr = addr
        self.name = name
        self.signed = signed
        self.multiplier = multiplier
        self.round = round
        self.test = False
        self.bitmap = None

    def value_in(self, v):
        if self.signed and v >= 32768:
            v -= 65536
        if self.multiplier is not None:
            v *= self.multiplier
        if self.round is not None:
            v = round(v, self.round)
        return v

REGISTERS = (
    LTOReg(3, 'fan_speed_supply'),
    LTOReg(4, 'fan_speed_exhaust'),
    LTOReg(6, 'temp_air_intake', signed=True, multiplier=0.1, round=3),
    LTOReg(7, 'temp_air_supply_hrc', signed=True, multiplier=0.1, round=3),
    LTOReg(8, 'temp_air_supply', signed=True, multiplier=0.1, round=3),
    LTOReg(9, 'temp_air_out_post', signed=True, multiplier=0.1, round=3),
    LTOReg(10, 'temp_air_out_pre', signed=True, multiplier=0.1, round=3),

    LTOReg(12, 'temp_water_return', signed=True, multiplier=0.1, round=3),
    LTOReg(13, 'hum_air_out_pre'),

    LTOReg(29, 'eff_supply'),
    LTOReg(30, 'eff_exhaust'),
    LTOReg(35, 'hum_air_out_pre_48h'),

    LTOReg(47, 'temp_air_setpoint', signed=True, multiplier=0.1, round=3),
    LTOReg(134, 'temp_air_intake_avg', signed=True, multiplier=0.1, round=3),
    LTOReg(137, 'summer_winter_threshold', signed=True, multiplier=0.1, round=3),

    LTOReg(782, 'ctrl_ao3_v_cool_water', signed=True, multiplier=0.1, round=3),
    LTOReg(784, 'ctrl_ao5_v_heat_water', signed=True, multiplier=0.1, round=3),
)

class MqttRelay(object):
    def __init__(self, cfg):
        self.in_startup_delay = True
        self.cfg = cfg
        graphite_cfg = cfg.get("graphite")
        
        self.graphite = None
        
        if graphite_cfg:
           self.graphite = mqtt_graphite.GraphiteSender(
               logging.getLogger('graphite'),
               graphite_cfg.get("host"), graphite_cfg.get("port"),
               ssl=True,
               ssl_ca_file=graphite_cfg.get("tls_ca_file", "cacert.pem"),
               ssl_key_file=graphite_cfg.get("tls_key_file", "meter-key.pem"),
               ssl_cert_file=graphite_cfg.get("tls_cert_file", "meter-cert.pem")
               )

        self.mqtt = None

        self.modbus = ModbusClient(host=cfg["lto_modbus"]["tcp_addr"], port=502, unit_id=1, auto_open=True)

    def on_mqtt_connect(self, client, userdata, flags, rc):
        log.info("mqtt connected: %r", rc)
        
    def on_mqtt_disconnect(self, client, userdata, rc):
        log.info("mqtt disconnected: %r", rc)
        
    def on_mqtt_message(self, client, userdata, message):
        """
        New MQTT message received
        """
        topic = message.topic
        try:
            payload = message.payload.decode("ascii")
        except Exception as e:
            log.info("mqtt message decoding fail on %r: %s", topic, e)
            return
    
    def value_out(self, key, value):
        topic_graphite = GRAPHITE_PREFIX + key
        log.debug("%s: %f", topic_graphite, value)
        if self.graphite:
            self.graphite.send(topic_graphite, value)
        if self.mqtt:
            topic_mqtt = topic_graphite.replace('.', '/')
            self.mqtt.publish(topic_mqtt, str(value))

    def fetch_data(self):
        register_batch = []
        prev_addr = None
        for r in REGISTERS:
            if register_batch and prev_addr != r.addr - 1:
                self.read_batch(register_batch)
                register_batch = []
                prev_addr = None
            register_batch.append(r)
            prev_addr = r.addr
        
        if register_batch:
            self.read_batch(register_batch)
    
    def read_batch(self, register_batch):
        #log.info("Reading: %r", [r.addr for r in register_batch])
        first = register_batch[0]
        values = self.modbus.read_holding_registers(first.addr, len(register_batch))
        for i in range(len(register_batch)):
            r = register_batch[i]
            v = r.value_in(values[i])
            if v is not None:
                self.value_out(r.name, v)

    def main_loop(self):
        next_tx_t = time.monotonic()
        mqtt_cfg = self.cfg.get("mqtt")

        if mqtt_cfg:
            try:
                client = mqtt.Client(APPNAME)
                client.on_connect = self.on_mqtt_connect
                client.on_disconnect = self.on_mqtt_disconnect
                client.connect(mqtt_cfg["host"], port=mqtt_cfg["port"], keepalive=30)
            except Exception as e:
                log.error("mqtt init exception: %r", e)
                return

            log.info("opened mqtt, subscribing")
            client.on_message=self.on_mqtt_message
            
            log.info("mqtt subscribed, running loop")
            client.loop_start()
            self.mqtt = client
        
        time.sleep(2)
        self.in_startup_delay = False
        log.info("startup delay done")
        
        while True:
            time.sleep(1)
            
            now = time.monotonic()
            
            if now >= next_tx_t:
                self.fetch_data()
                # avoid busy looping
                next_t = next_tx_t + TX_INTERVAL
                if time.monotonic() >= next_t:
                    next_tx_t = time.monotonic() + TX_INTERVAL
                else:
                    next_tx_t = next_t

signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT, cleanup)

try:
    relay = MqttRelay(cfg)
    relay.main_loop()
except KeyboardInterrupt:
    log.info("Interrupted by keypress")
    sys.exit(0)

