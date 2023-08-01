
import queue
import threading
import time

import graphitesend

# hold items queued for some time, over short network outages
max_queue_size = 7200

g_thread = None

class GraphiteThread(object):
    def __init__(self, log, hostname, port, ssl=False, ssl_ca_file=None, ssl_key_file = None, ssl_cert_file=None):
        self.log = log
        self.hostname = hostname
        self.port = port
        self.ssl = ssl
        self.ssl_ca_file = ssl_ca_file
        self.ssl_key_file = ssl_key_file
        self.ssl_cert_file = ssl_cert_file
        
        self.graphite = None
        self.thr = None
        
        self.queue = queue.Queue(max_queue_size)
        self.stopping = threading.Event()
        
        self.check_connect()
        
        t = threading.Thread(target = self.__consume)
        t.daemon = True
        t.start()
        
    def check_connect(self):
        self.log.debug("check_connect")
        if self.graphite == None or self.graphite.socket == None:
            try:
                self.log.info("Connecting to Graphite")
                self.graphite = graphitesend.GraphiteClient(
                    fqdn_squash=True, graphite_server=self.hostname, graphite_port=self.port,
                    use_ssl=self.ssl, ssl_ca_file=self.ssl_ca_file,
                    ssl_key_file=self.ssl_key_file, ssl_cert_file=self.ssl_cert_file)
            except Exception as e:
                self.log.error("Failed to connect to Graphite: %r" % e)
                self.graphite = None
                return
    
    def __consume(self):
        while not self.stopping.is_set():
            try:
                item = self.queue.get(block = True, timeout = 20)
            except queue.Empty:
                #self.log.info("empty queue")
                self.check_connect()
                continue
                
            metric, value, timestamp = item
            self.log.debug("task found: %s %s %d", metric, value, timestamp)
            
            transmitted = False
            while not transmitted and not self.stopping.is_set():
                try:
                    self.check_connect()
                    
                    self.log.debug("task transmit")
                    transmitted = self.transmit(metric, value, timestamp)
                    self.log.debug("task done, %s", "OK" if transmitted else "fail")
                except Exception as e:
                    import traceback
                    self.log.error("GraphiteThread: %s", traceback.format_exc())
                
                if not transmitted:
                    time.sleep(10)
                
            self.queue.task_done()
                
        self.log.debug("GraphiteThread stopping")

    def transmit(self, metric, value, timestamp=None):
        if self.graphite == None:
            self.log.info("Graphite transmit: no graphite")
            return False
            
        try:
            self.graphite.send(metric, value, timestamp)
        except graphitesend.GraphiteSendException:
            self.log.exception("Graphite send failed (sendexc)")
            #try:
            #    self.graphite.disconnect()
            #except Exception:
            #    pass
            self.log.info("Graphite transmit: clearing graphite after exception")
            self.graphite = None
            return False
        except Exception:
            self.log.exception("Graphite send failed (unexpected exception)")
            try:
                self.graphite.disconnect()
            except Exception:
                pass
            self.log.info("Graphite transmit: clearing graphite after exception")
            self.graphite = None
            return False
            
        self.log.debug("Graphite transmit ok")
        return True
 
class GraphiteSender(object):
    def __init__(self, log, hostname, port, ssl=False, ssl_ca_file=None, ssl_key_file=None, ssl_cert_file=None):
        self.log = log
        self.hostname = hostname
        self.port = port
        
        global g_thread
        if g_thread == None:
            g_thread = GraphiteThread(log, self.hostname, self.port,
                ssl=ssl, ssl_ca_file=ssl_ca_file, ssl_key_file=ssl_key_file, ssl_cert_file=ssl_cert_file)
        

    def send(self, metric, value):
        global g_thread
        
        # timestamp before queuing so that things will line up correctly
        timestamp = int(time.time())

        try:
            g_thread.queue.put((metric, value, timestamp), block = True, timeout = 0.1)
            return True
        except Queue.Full:
            self.log.error("GraphiteSender: queue full")
            return False

