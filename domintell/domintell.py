"""pydomintell - Python implementation of the Domintell Gateway."""
import re
import logging
import os
import pickle
import select
import socket
import threading
import time
from collections import deque
from importlib import import_module
from queue import Queue

_LOGGER = logging.getLogger(__name__)

# pylint: disable=too-many-lines

class Gateway(object):
    """Base implementation for a Domintell Gateway."""

    # pylint: disable=too-many-instance-attributes

    def __init__(self, event_callback=None, persistence=False,
                 persistence_file='domintell.pickle',):
        self.queue = Queue()
        self.lock = threading.Lock()
        self.event_callback = event_callback
        self.sensors = {}
        self.debug = False  # if true - print all received messages
        self.persistence = persistence  # if true - save sensors to disk
        self.persistence_file = persistence_file  # path to persistence file
        self.persistence_bak = '{}.bak'.format(self.persistence_file)
        if persistence:
            self._safe_load_sensors()

    def send(self, message):
        """Should be implemented by a child class."""
        raise NotImplementedError

    def logic(self, data):
        """Parse the data and respond to it appropriately.

        Response is returned to the caller and has to be sent
        data as a command string.
        """
        ret = None
        try:
            regex = r"([A-Z0-9]{3}[A-F0-9 ]{6}[IODTCSB]{1}.*\r)"
            regexClock = r"([0-9]{2}:[0-9 ]{2} [0-9]{2}/[0-9]{2}/[0-9]{2}\r)"

            if data == "PONG\r":
                pass

            elif re.search(regex, data):
                data = data[:-1]
                module = data[:3]
                serial = data[3:9]
                type = data[9]
                value = data[10:]

                if module == "IS8" or module == "IS4" or module == "DET":
                    if module == "IS8":
                        num_sensor = 8
                    elif module == "IS4":
                        num_sensor = 4
                    else:
                        num_sensor = 1
                    for input in range(1, num_sensor+1):
                        inputStr = data[:9]
                        inputStr = inputStr.replace(' ', '0')
                        inputStr = inputStr + "-" + str(input)
                        #print(inputStr)

                        if inputStr not in self.sensors:
                            self.sensors[inputStr] = { "id": inputStr,
                                                       "type": "input",
                                                       "desc": "",
                                                       "value": "" }
                            print("New sensor: ", inputStr)

                        if int(value, 16) & 1 << (input-1):
                            newValue = "on"
                        else:
                            newValue = "off"

                        if self.sensors[inputStr]["value"] != newValue:
                            self.sensors[inputStr]["value"] = newValue
                            self.alert(inputStr)

                elif module == "BU1" or module == "BU2" or \
                     module == "BU4" or module == "BU6":
                    num_sensor = int(module[2])
                    if type == "I":
                        for input in range(1, num_sensor+1):
                            inputStr = data[:9]
                            inputStr = inputStr.replace(' ', '0')
                            inputStr = inputStr + "-" + str(input)
                            #print(inputStr)

                            if inputStr not in self.sensors:
                                self.sensors[inputStr] = { "id": inputStr,
                                                           "type": "input",
                                                           "desc": "",
                                                           "value": "" }
                                print("New sensor: ", inputStr)

                            if int(value, 16) & 1 << (input-1):
                                newValue = "on"
                            else:
                                newValue = "off"

                            if self.sensors[inputStr]["value"] != newValue:
                                self.sensors[inputStr]["value"] = newValue
                                self.alert(inputStr)
                    else:
                        for output in range(num_sensor+1, (2*num_sensor)+1):
                            outputStr = data[:9]
                            outputStr = outputStr.replace(' ', '0')
                            outputStr = outputStr + "-" + str(hex(output))[2:].upper()
                            #print(outputStr)

                            if outputStr not in self.sensors:
                                self.sensors[outputStr] = { "id": outputStr,
                                                            "type": "output",
                                                            "desc": "",
                                                            "value": "" }
                                print("New output: ", outputStr)

                            if int(value, 16) & 1 << (output-num_sensor-1):
                                newValue = "on"
                            else:
                                newValue = "off"

                            if self.sensors[outputStr]["value"] != newValue:
                                self.sensors[outputStr]["value"] = newValue
                                self.alert(outputStr)

                elif module == "BIR" or module == "DMR":
                    if module == "BIR":
                        num_sensor = 8
                    else:
                        num_sensor = 5

                    for output in range(1, num_sensor+1):
                        outputStr = data[:9]
                        outputStr = outputStr.replace(' ', '0')
                        outputStr = outputStr + "-" + str(output)
                        #print(outputStr)

                        if outputStr not in self.sensors:
                            self.sensors[outputStr] = { "id": outputStr,
                                                        "type": "output",
                                                        "desc": "",
                                                        "value": "" }
                            print("New output: ", outputStr)

                        if int(value, 16) & 1 << (output-1):
                            newValue = "on"
                        else:
                            newValue = "off"

                        if self.sensors[outputStr]["value"] != newValue:
                            self.sensors[outputStr]["value"] = newValue
                            self.alert(outputStr)

                elif module == "DIM":
                    value = value.replace(' ', '0')
                    for output in range(1, 9):
                        outputStr = data[:9]
                        outputStr = outputStr.replace(' ', '0')
                        outputStr = outputStr + "-" + str(output)

                        if outputStr not in self.sensors:
                            self.sensors[outputStr] = { "id": outputStr,
                                                        "type": "output",
                                                        "desc": "",
                                                        "value": "" }
                            print("New output: ", outputStr)

                        valueStr = value[(output-1)*2:((output-1)*2)+2]
                        newValue = int(valueStr, 16)
                        if self.sensors[outputStr]["value"] != newValue:
                            self.sensors[outputStr]["value"] = newValue
                            self.alert(outputStr)

                elif module == "AMP":
                    ampStr = data[:9]
                    ampStr = ampStr.replace(' ', '0')
                    ampStr = ampStr + "-" + data[10]

                    if ampStr not in self.sensors:
                        self.sensors[ampStr] = { "id": ampStr,
                                                 "type": "ampli",
                                                 "desc": "",
                                                 "value": "" }
                        print("New sensor: ", ampStr)

                    newValue = data[12:]

                    if self.sensors[ampStr]["value"] != newValue:
                        self.sensors[ampStr]["value"] = newValue
                        self.alert(ampStr)

                elif module == "VAR" or module == "SYS":
                    pass

                elif module == "SFE":
                    #Seems like some of the SFE from APPINFO arrive here
                    pass

                else:
                    print("Unknown: ", data)
                    pass

            elif re.search(regexClock, data):
                if "clock" not in self.sensors:
                    self.sensors["clock"] = { "id": "clock",
                                              "type": "clock",
                                              "desc": "",
                                              "value": "" }

                self.sensors["clock"]["value"] = data
                self.alert("clock")

            elif data.startswith("IS8") or \
                 data.startswith("IS4") or \
                 data.startswith("DET"):
                #print("APPINFO: ", data)
                data = data[:-1]
                sensorStr = data[0:11]
                sensorStr = sensorStr.replace(' ', '0')
                module = data[:3]
                value = data[11:].split("[")[0]

                if sensorStr not in self.sensors:
                    #print("APPINFO: ", data)
                    self.sensors[sensorStr] = { "id": sensorStr,
                                                "type": "input",
                                                "desc": value,
                                                "value": "" }
                    #print("Sensor: ", self.sensors[sensorStr])
                else:
                    self.sensors[sensorStr]["desc"] = value;
                    #print("Sensor: ", self.sensors[sensorStr])

            elif data.startswith("BIR") or \
                 data.startswith("DMR") or \
                 data.startswith("DIM"):
                #print("APPINFO: ", data)
                data = data[:-1]
                sensorStr = data[0:11]
                sensorStr = sensorStr.replace(' ', '0')
                module = data[:3]
                value = data[11:].split("[")[0]

                if sensorStr not in self.sensors:
                    #print("APPINFO: ", data)
                    self.sensors[sensorStr] = { "id": sensorStr,
                                                "type": "output",
                                                "desc": value,
                                                "value": "" }
                    #print("Sensor: ", self.sensors[sensorStr])
                else:
                    self.sensors[sensorStr]["desc"] = value;
                    #print("Sensor: ", self.sensors[sensorStr])

            elif data.startswith("BU1") or \
                 data.startswith("BU2") or \
                 data.startswith("BU4") or \
                 data.startswith("BU6"):
                #print("APPINFO: ", data)
                num_channels = int(data[2])
                channel = int(data[10], 16)
                data = data[:-1]
                sensorStr = data[0:11]
                sensorStr = sensorStr.replace(' ', '0')
                module = data[:3]
                value = data[11:].split("[")[0]

                if channel > num_channels:
                    type = "output"
                else:
                    type = "input"

                if sensorStr not in self.sensors:
                    #print("APPINFO: ", data)
                    self.sensors[sensorStr] = { "id": sensorStr,
                                                "type": type,
                                                "desc": value,
                                                "value": "" }
                    #print("Sensor: ", self.sensors[sensorStr])
                else:
                    self.sensors[sensorStr]["desc"] = value;
                    #print("Sensor: ", self.sensors[sensorStr])

            elif data.startswith("AMP"):
                #print("APPINFO: ", data)
                data = data[:-1]
                sensorStr = data[0:11]
                sensorStr = sensorStr.replace(' ', '0')
                module = data[:3]
                value = data[11:].split("[")[0]

                if sensorStr not in self.sensors:
                    #print("APPINFO: ", data)
                    self.sensors[sensorStr] = { "id": sensorStr,
                                                "type": "ampli",
                                                "desc": value,
                                                "value": "" }
                    #print("Sensor: ", self.sensors[sensorStr])
                else:
                    self.sensors[sensorStr]["desc"] = value;
                    #print("Sensor: ", self.sensors[sensorStr])

            elif data.startswith("END APPINFO"):
                self.sock.sendto(bytes("PING", 'UTF-8'),
                                 self.server_address)

            elif data.startswith("STA") or \
                 data.startswith("APPINFO") or \
                 data.startswith("SFE") or \
                 data.startswith("ET2") or \
                 data.startswith("VAR") or \
                 data.startswith("SYS") or \
                 data.startswith("MEM"):
                pass

            else:
                print("Unknown: ", data)

        except ValueError:
            return

    def _save_pickle(self, filename):
        """Save sensors to pickle file."""
        with open(filename, 'wb') as file_handle:
            pickle.dump(self.sensors, file_handle, pickle.HIGHEST_PROTOCOL)
            file_handle.flush()
            os.fsync(file_handle.fileno())

    def _load_pickle(self, filename):
        """Load sensors from pickle file."""
        with open(filename, 'rb') as file_handle:
            self.sensors = pickle.load(file_handle)

    def _save_sensors(self):
        """Save sensors to file."""
        fname = os.path.realpath(self.persistence_file)
        exists = os.path.isfile(fname)
        dirname = os.path.dirname(fname)
        if exists and os.access(fname, os.W_OK) and \
           os.access(dirname, os.W_OK) or \
           not exists and os.access(dirname, os.W_OK):
            split_fname = os.path.splitext(fname)
            tmp_fname = '{}.tmp{}'.format(split_fname[0], split_fname[1])
            self._perform_file_action(tmp_fname, 'save')
            if exists:
                os.rename(fname, self.persistence_bak)
            os.rename(tmp_fname, fname)
            if exists:
                os.remove(self.persistence_bak)
        else:
            _LOGGER.error('Permission denied when writing to %s', fname)

    def _load_sensors(self, path=None):
        """Load sensors from file."""
        if path is None:
            path = self.persistence_file
        exists = os.path.isfile(path)
        if exists and os.access(path, os.R_OK):
            if path == self.persistence_bak:
                os.rename(path, self.persistence_file)
                path = self.persistence_file
            self._perform_file_action(path, 'load')
            return True
        else:
            print('File does not exist or is not readable: %s', path)
            return False

    def _safe_load_sensors(self):
        """Load sensors safely from file."""
        try:
            loaded = self._load_sensors()
        except (EOFError, ValueError):
            print('Bad file contents: %s', self.persistence_file)
            loaded = False
        if not loaded:
            print('Trying backup file: %s', self.persistence_bak)
            try:
                if not self._load_sensors(self.persistence_bak):
                    print('Failed to load sensors from file: %s',
                                    self.persistence_file)
            except (EOFError, ValueError):
                print('Bad file contents: %s', self.persistence_file)
                print('Removing file: %s', self.persistence_file)
                os.remove(self.persistence_file)

    def _perform_file_action(self, filename, action):
        """Perform action on specific file types.

        Dynamic dispatch function for performing actions on
        specific file types.
        """
        ext = os.path.splitext(filename)[1]
        func = getattr(self, '_%s_%s' % (action, ext[1:]), None)
        if func is None:
            raise Exception('Unsupported file type %s' % ext[1:])
        func(filename)

    def alert(self, nid):
        """Tell anyone who wants to know that a sensor was updated.

        Also save sensors if persistence is enabled.
        """
        if self.event_callback is not None:
            try:
                self.event_callback('sensor_update', nid)
            except Exception as exception:  # pylint: disable=W0703
                _LOGGER.exception(exception)

        if self.persistence:
            self._save_sensors()

    def handle_queue(self, queue=None):
        """Handle queue.

        If queue is not empty, get the function and any args and kwargs
        from the queue. Run the function and return output.
        """
        if queue is None:
            queue = self.queue
        if not queue.empty():
            func, args, kwargs = queue.get()
            reply = func(*args, **kwargs)
            queue.task_done()
            return reply

    def fill_queue(self, func, args=None, kwargs=None, queue=None):
        """Put a function in a queue.

        Put the function 'func', a tuple of arguments 'args' and a dict
        of keyword arguments 'kwargs', as a tuple in the queue.
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        if queue is None:
            queue = self.queue
        queue.put((func, args, kwargs))

    def set_value(self, sensor_id, child_id, value_type, value, **kwargs):
        if sensor_id not in self.sensors:
            return

        if self.sensors[sensor_id]["type"] != "output":
            return

        if sensor_id.startswith("BU"):
            num_channels = int(sensor_id[2])
            channel = int(sensor_id[10], 16)
            if channel > num_channels:
                sensor_id = sensor_id[0:10] + str(channel - num_channels)

        if value == 1 or value == "on":
             command = sensor_id + "%I"
        else:
             command = sensor_id + "%O"
        
        self.sock.sendto(bytes(command, 'UTF-8'), self.server_address)
        return command

class Deth01Gateway(Gateway, threading.Thread):
    """Domintell UDP ethernet gateway."""

    # pylint: disable=too-many-arguments

    def __init__(self, host, event_callback=None,
                 persistence=False, persistence_file='domintell.pickle',
                 port=17481, timeout=1.0,
                 reconnect_timeout=10.0):
        """Setup UDP ethernet gateway."""
        threading.Thread.__init__(self)
        Gateway.__init__(self, event_callback, persistence,
                         persistence_file)
        self.sock = None
        self.server_address = (host, port)
        self.timeout = timeout
        self.reconnect_timeout = reconnect_timeout
        self._stop_event = threading.Event()

    def connect(self):
        """Connect to the Domintell system, on host and port."""
        if self.sock == None:
            self.sock = socket.socket(socket.AF_INET, # Internet
                                      socket.SOCK_DGRAM) # UDP
            self.sock.setsockopt(socket.SOL_SOCKET,
                                 socket.SO_REUSEADDR,
                                 1)

        self.sock.sendto(bytes("LOGIN", 'UTF-8'), self.server_address)
        time.sleep(0.02)  # short sleep to avoid burning 100% cpu
        try:
            available_socks = self._check_socket()
        except OSError:
            print('Server socket %s has an error.', self.sock)
            self.disconnect()
            return False
        if available_socks[0] and self.sock is not None:
            string = self.recv_timeout()
            line = string.rstrip()
            if line == "INFO:Session opened:INFO":
                print(line)
                self.sock.sendto(bytes("APPINFO", 'UTF-8'),
                                 self.server_address)
                return True

        self.disconnect()
        return False

    def disconnect(self):
        """Close the socket."""
        if not self.sock:
            return
        _LOGGER.info('Closing socket at %s.', self.server_address)
        #self.sock.shutdown(socket.SHUT_WR)
        self.sock.close()
        self.sock = None
        _LOGGER.info('Socket closed at %s.', self.server_address)

    def stop(self):
        """Stop the background thread."""
        _LOGGER.info('Stopping thread')
        self._stop_event.set()

    def _check_socket(self, sock=None, timeout=None):
        """Check if socket is readable/writable."""
        if sock is None:
            sock = self.sock
        available_socks = select.select([sock], [sock], [sock], timeout)
        if available_socks[2]:
            raise OSError
        return available_socks

    def recv_timeout(self):
        """Receive reply from server, with a timeout."""
        # make socket non blocking
        self.sock.setblocking(False)
        # total data in an array
        total_data = []
        data_string = ''
        joined_data = ''
        # start time
        begin = time.time()

        while not data_string.endswith('\n'):
            data_string = ''
            # break after timeout
            if time.time() - begin > self.timeout:
                break
            # receive data
            try:
                data_bytes = self.sock.recv(1024)
                if data_bytes:
                    data_string = data_bytes.decode('utf-8')
                    _LOGGER.debug('Received %s', data_string)
                    total_data.append(data_string)
                    # reset start time
                    begin = time.time()
                    # join all data to final data
                    joined_data = ''.join(total_data)
                else:
                    # sleep to add time difference
                    time.sleep(0.1)
            except OSError:
                _LOGGER.error('Receive from server failed.')
                self.disconnect()
                break
            except ValueError:
                _LOGGER.warning(
                    'Error decoding message from gateway, '
                    'probably received bad byte.')
                break

        return joined_data

    def send(self, message):
        """Write a command string to the gateway via the socket."""
        if not message:
            return
        with self.lock:
            try:
                # Send data
                _LOGGER.debug('Sending %s', message)
                self.sock.sendall(message.encode())

            except OSError:
                # Send failed
                _LOGGER.error('Send to server failed.')
                self.disconnect()

    def run(self):
        """Background thread that reads messages from the gateway."""
        self.lastKeepalive = 0;

        while not self._stop_event.is_set():
            if self.sock is None and not self.connect():
                print('Waiting %s secs before trying to connect again.', self.reconnect_timeout)
                time.sleep(self.reconnect_timeout)
                continue

            if self.lastKeepalive == 0 or time.time() - self.lastKeepalive > 60:
                self.sock.sendto(bytes("PING", 'UTF-8'),
                                 self.server_address)
                self.lastKeepalive = time.time();
 
            try:
                available_socks = self._check_socket()
            except OSError:
                print('Server socket %s has an error.', self.sock)
                self.disconnect()
                continue
            if available_socks[1] and self.sock is not None:
                response = self.handle_queue()
                if response is not None:
                    self.send(response)
            if not self.queue.empty():
                continue
            time.sleep(0.02)  # short sleep to avoid burning 100% cpu
            if available_socks[0] and self.sock is not None:
                string = self.recv_timeout()
                lines = string.split('\n')
                #print(lines)
                # Throw away last empty line or uncompleted message.
                del lines[-1]
                for line in lines:
                    self.fill_queue(self.logic, (line,))
        self.disconnect()
