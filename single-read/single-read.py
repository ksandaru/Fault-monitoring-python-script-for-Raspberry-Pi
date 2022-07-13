# Serial Pheriperal Interface communication with UHF RFID reader and PC/Raspberry pi
# MySQL Database connection and data storing in to Deployed database
import serial
import abc
import socket
from enum import Enum

import sys
import mysql.connector
from datetime import datetime

db = mysql.connector.connect(
    host="bttyzkjbqqcjy1zpvo5q-mysql.services.clever-cloud.com",
    user="ubqcdhetqwgprcw7",
    password="L7sA3pf0j08q7EecB474",
    database="bttyzkjbqqcjy1zpvo5q"
)

PRESET_VALUE = 0xFFFF
POLYNOMIAL = 0x8408


def checksum(pucY):
    uiCrcValue = PRESET_VALUE
    for ucY in pucY:
        uiCrcValue = uiCrcValue ^ ucY
        for ucJ in range(8):
            if uiCrcValue & 0x0001:
                uiCrcValue = (uiCrcValue >> 1) ^ POLYNOMIAL;
            else:
                uiCrcValue = (uiCrcValue >> 1)
    return uiCrcValue;


# --------------------------------------------------
G2_TAG_INVENTORY = 0x01
G2_READ_DATA = 0x02
G2_WRITE_DATA = 0x03
G2_WRITE_EPC = 0x04
G2_KILL_TAG = 0x05
G2_SET_PROTECTION = 0x06
G2_ERASE_BLOCK = 0x07
G2_READ_PROTECTION_EPC = 0x08
G2_READ_PROTECTION_NO_EPC = 0x09
G2_UNLOCK_READ_PROTECTION = 0x0a
G2_READ_PROTECTION_STATUS_CHECK = 0x0b
G2_EAS_CONFIGURATION = 0x0c
G2_EAS_ALERT_DETECTION = 0x0d
G2_SINGLE_TAG_INVENTORY = 0x0f
G2_WRITE_BLOCKS = 0x10
G2_GET_MONZA_4QT_WORKING_PARAMETERS = 0x11
G2_SET_MONZA_4QT_WORKING_PARAMETERS = 0x12
G2_READ_EXTENDED_DATA = 0x15
G2_WRITE_EXTENDED_DATA = 0x16
G2_TAG_INVENTORY_WITH_MEMORY_BUFFER = 0x18
G2_MIX_INVENTORY = 0x19
G2_INVENTORY_EPC = 0x1a
G2_QT_INVENTORY = 0x1b

CF_GET_READER_INFO = 0x21
CF_SET_WORKING_FREQUENCY = 0x22
CF_SET_READER_ADDRESS = 0x24
CF_SET_READER_INVENTORY_TIME = 0x25
CF_SET_SERIAL_BAUD_RATE = 0x28
CF_SET_RF_POWER = 0x2f
CF_SET_WORK_MODE_288M = 0x76
CF_SET_WORK_MODE_18 = 0x35
CF_SET_BUZZER_ENABLED = 0x40
CF_SET_ACCOUSTO_OPTIC_TIMES = 0x33

# -----------------------------------

G2_TAG_INVENTORY_PARAM_MEMORY_EPC = 0x01
G2_TAG_INVENTORY_PARAM_MEMORY_TID = 0x02
G2_TAG_INVENTORY_PARAM_MEMORY_USER = 0x03

G2_TAG_INVENTORY_PARAM_SESSION_S0 = 0x00
G2_TAG_INVENTORY_PARAM_SESSION_S1 = 0x01
G2_TAG_INVENTORY_PARAM_SESSION_S2 = 0x02
G2_TAG_INVENTORY_PARAM_SESSION_S3 = 0x03
G2_TAG_INVENTORY_PARAM_SESSION_SMART = 0xFF

G2_TAG_INVENTORY_PARAM_ANTENNA_1 = 0x80
G2_TAG_INVENTORY_PARAM_ANTENNA_2 = 0x81
G2_TAG_INVENTORY_PARAM_ANTENNA_3 = 0x82
G2_TAG_INVENTORY_PARAM_ANTENNA_4 = 0x83

G2_TAG_INVENTORY_PARAM_TARGET_A = 0x00
G2_TAG_INVENTORY_PARAM_TARGET_B = 0x01


class ReaderCommand(object):

    def __init__(self, cmd, addr=0xFF, data=[]):
        self.addr = addr
        self.cmd = cmd
        self.data = data

    def serialize(self):
        frame_length = 4 + len(self.data)
        base_data = bytearray([frame_length, self.addr, self.cmd]) + bytearray(self.data)
        base_data.extend(bytearray(self.checksum_bytes(base_data)))
        return base_data

    def checksum_bytes(self, data_bytes):
        crc = checksum(data_bytes)
        crc_msb = crc >> 8
        crc_lsb = crc & 0xFF
        return bytearray([crc_lsb, crc_msb])


class G2InventoryCommand(ReaderCommand):

    def __init__(self, addr=0xFF, q_value=15, deliver_statistics=0, strategy=0, fast_id=0,
                 session=G2_TAG_INVENTORY_PARAM_SESSION_S0, mask_source=G2_TAG_INVENTORY_PARAM_MEMORY_EPC,
                 target=G2_TAG_INVENTORY_PARAM_TARGET_A, antenna=G2_TAG_INVENTORY_PARAM_ANTENNA_1, scan_time=0x14):
        # TODO check q_value in range 0 ~ 15, session 0 ~ 3
        mask_data = [0x00, 0x00, 0x00]
        q_value_byte = (deliver_statistics << 7) + (strategy << 6) + (fast_id << 5) + q_value
        cmd_data = [q_value_byte, session, mask_source] + mask_data + [target, antenna, scan_time]
        super(G2InventoryCommand, self).__init__(G2_TAG_INVENTORY, addr, data=cmd_data)


def _translate_antenna_num(antenna_code):
    if antenna_code == 1:
        return 1
    elif antenna_code == 2:
        return 2
    elif antenna_code == 4:
        return 3
    elif antenna_code == 8:
        return 4
    else:
        return None


class G2InventoryResponse(object):
    frame_class = None

    def __init__(self, resp_bytes):
        self.resp_bytes = resp_bytes

    def get_frame(self):
        offset = 0
        while offset < len(self.resp_bytes):
            next_frame = self.frame_class(self.resp_bytes, offset=offset)
            offset += len(next_frame) + 1  # For a frame with stated length N there are N+1 bytes
            yield next_frame

    def get_tag(self):
        for response_frame in self.get_frame():
            for tag in response_frame.get_tag():
                yield tag


class ReaderResponseFrame(object):

    def __init__(self, resp_bytes, offset=0):
        if len(resp_bytes) < 5:
            raise ValueError('Response must be at least 5 bytes')
        self.len = resp_bytes[offset]
        if self.len + offset > len(resp_bytes) - 1:
            raise ValueError(
                'Response does not contain enough bytes for frame (expected %d bytes after offset %d, actual length %d)' % (
                self.len, offset, len(resp_bytes)))
        self.reader_addr = resp_bytes[offset + 1]
        self.resp_cmd = resp_bytes[offset + 2]
        self.result_status = resp_bytes[offset + 3]
        self.data = resp_bytes[offset + 4:offset + self.len - 1]
        cs_status = self.verify_checksum(resp_bytes[offset:offset + self.len - 1],
                                         resp_bytes[offset + self.len - 1:offset + self.len + 1])
        if cs_status is not True:
            raise (ValueError('Checksum does not match'))

    def verify_checksum(self, data_bytes, checksum_bytes):
        data_crc = checksum(bytearray(data_bytes))
        crc_msb = data_crc >> 8
        crc_lsb = data_crc & 0xFF
        return checksum_bytes[0] == crc_lsb and checksum_bytes[1] == crc_msb

    def __len__(self):
        return self.len

    def get_data(self):
        return self.data


class G2InventoryResponseFrame18(ReaderResponseFrame):

    def __init__(self, resp_bytes, offset=0):
        super(G2InventoryResponseFrame18, self).__init__(resp_bytes, offset)
        self.num_tags = 0
        if len(self.data) > 0:
            self.num_tags = self.data[0]

    def get_tag(self):
        if len(self.data) > 1:
            tag_data = TagData(self.data, prefix_bytes=0, suffix_bytes=0)
            for data_item in tag_data.get_tag_data():
                epc_value = data_item[1]
                yield Tag(epc_value)


class G2InventoryResponse(G2InventoryResponse):
    frame_class = G2InventoryResponseFrame18


# -----------------------------------------------------

class G2InventoryResponseFrame288(ReaderResponseFrame):
    tag_prefix_bytes = 0
    tag_suffix_bytes = 1

    tag_data_prefix_bytes = 1  # Number of bytes before 'num tags' byte

    def __init__(self, resp_bytes, offset=0):
        super(G2InventoryResponseFrame288, self).__init__(resp_bytes, offset)
        self.num_tags = 0
        self.antenna = 0
        if len(self.data) > self.tag_data_prefix_bytes:
            self.antenna = _translate_antenna_num(self.data[0])
            self.num_tags = self.data[1]

    def get_tag(self):
        tag_data_prefix_and_num_tags_bytes = self.tag_data_prefix_bytes + 1
        if len(self.data) > tag_data_prefix_and_num_tags_bytes:
            tag_data = TagData(self.data[self.tag_data_prefix_bytes:], prefix_bytes=self.tag_prefix_bytes,
                               suffix_bytes=self.tag_suffix_bytes)
            for data_item in tag_data.get_tag_data():
                epc_value = data_item[1]
                rssi = data_item[2][0]
                yield Tag(epc_value, antenna_num=self.antenna, rssi=rssi)


class G2InventoryResponse(G2InventoryResponse):
    frame_class = G2InventoryResponseFrame288


# ---------------------------------------------------------


class BaseTransport(object):
    __metaclass__ = abc.ABCMeta
    read_bytecount = 0x100

    def __init__(self):
        raise NotImplementedError

    @abc.abstractmethod
    def read_bytes(self, length):
        raise NotImplementedError

    @abc.abstractmethod
    def write_bytes(self, byte_array):
        raise NotImplementedError

    def read(self):
        return self.read_bytes(self.read_bytecount)

    def read_frame(self):
        length_bytes = self.read_bytes(1)
        frame_length = length_bytes[0]
        data = length_bytes + self.read_bytes(frame_length)
        return bytearray(data)

    def write(self, byte_array):
        self.write_bytes(byte_array)


class SerialTransport(BaseTransport):
    # Todo: change device port from 'COM3' to '/dev/ttyUSB0' if use Raspberry pi or any Linux OS
    def __init__(self, device='/dev/ttyUSB0', baud_rate=57600, timeout=5):
        self.serial = serial.Serial(device, baud_rate, timeout=timeout)

    def read_bytes(self, length):
        return bytearray(self.serial.read(length))

    def write_bytes(self, byte_array):
        self.serial.write(byte_array)

    def close(self):
        self.serial.close()


# ----------------------------------------


class TcpTransport(BaseTransport):
    buffer_size = 0xFF

    def __init__(self, reader_addr, reader_port, timeout=5, auto_connect=False):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(timeout)
        self.reader_addr = reader_addr
        self.reader_port = reader_port
        if auto_connect:
            self.connect()

    def connect(self):
        self.socket.connect((self.reader_addr, self.reader_port))

    def read_bytes(self, length):
        return self.socket.recv(length)

    def write_bytes(self, byte_array):
        self.socket.sendall(byte_array)

    def close(self):
        self.socket.close()


class MockTransport(BaseTransport):

    def __init__(self, data):
        self.pointer = 0
        self.data = bytes(data)

    def read_bytes(self, length):
        data = self.data[self.pointer:self.pointer + length]
        self.pointer += length
        return data

    def write_bytes(self, byte_array):
        pass

    def close(self):
        pass


# ----------------------------------------------

G2_TAG_INVENTORY_STATUS_COMPLETE = 0x01
G2_TAG_INVENTORY_STATUS_TIMEOUT = 0x02
G2_TAG_INVENTORY_STATUS_MORE_FRAMES = 0x03
G2_TAG_INVENTORY_STATUS_MEMORY_EXCEEDED = 0x04
G2_TAG_INVENTORY_STATUS_ANTENNA_ERROR = 0xF8
# -------------------------------------------


G2_TAG_INVENTORY = 0x01
G2_READ_DATA = 0x02
G2_WRITE_DATA = 0x03
G2_WRITE_EPC = 0x04
G2_KILL_TAG = 0x05
G2_SET_PROTECTION = 0x06
G2_ERASE_BLOCK = 0x07
G2_READ_PROTECTION_EPC = 0x08
G2_READ_PROTECTION_NO_EPC = 0x09
G2_UNLOCK_READ_PROTECTION = 0x0a
G2_READ_PROTECTION_STATUS_CHECK = 0x0b
G2_EAS_CONFIGURATION = 0x0c
G2_EAS_ALERT_DETECTION = 0x0d
G2_SINGLE_TAG_INVENTORY = 0x0f
G2_WRITE_BLOCKS = 0x10
G2_GET_MONZA_4QT_WORKING_PARAMETERS = 0x11
G2_SET_MONZA_4QT_WORKING_PARAMETERS = 0x12
G2_READ_EXTENDED_DATA = 0x15
G2_WRITE_EXTENDED_DATA = 0x16
G2_TAG_INVENTORY_WITH_MEMORY_BUFFER = 0x18
G2_MIX_INVENTORY = 0x19
G2_INVENTORY_EPC = 0x1a
G2_QT_INVENTORY = 0x1b

CF_GET_READER_INFO = 0x21
CF_SET_WORKING_FREQUENCY = 0x22
CF_SET_READER_ADDRESS = 0x24
CF_SET_READER_INVENTORY_TIME = 0x25
CF_SET_SERIAL_BAUD_RATE = 0x28
CF_SET_RF_POWER = 0x2f
CF_SET_WORK_MODE_288M = 0x76
CF_SET_WORK_MODE_18 = 0x35
CF_SET_BUZZER_ENABLED = 0x40
CF_SET_ACCOUSTO_OPTIC_TIMES = 0x33


# ------------------------------------------


class CommandRunner(object):

    def __init__(self, transport):
        self.transport = transport

    def run(self, command):
        self.transport.write(command.serialize())
        return self.transport.read_frame()


class ReaderFrequencyBand(Enum):
    China2 = 0b0001
    US = 0b0010
    Korea = 0b0011
    EU = 0b0100
    Ukraine = 0b0110
    Peru = 0b0111
    China1 = 0b1000
    EU3 = 0b1001
    Taiwan = 0b1010
    US3 = 0b1100


class ReaderType(Enum):
    RRU9803M = 0x03  # CF-RU5102 (desktop USB reader/writer, as specified)
    RRU9803M_1 = 0x08  # CF-RU5102 (desktop USB reader/writer, actual)
    UHFReader18 = 0x09
    UHFReader288M = 0x0c
    UHFReader86 = 0x0f  # CF-MU903/CF-MU904 (as documented)
    UHFReader86_1 = 0x10  # CF-MU903/CF-MU904 (actual)
    RRU9883M = 0x16  # CF-MU902
    UHFReader288MP = 0x20  # CF-MU804


class ReaderInfoFrame(ReaderResponseFrame):

    def __init__(self, resp_bytes):
        super(ReaderInfoFrame, self).__init__(resp_bytes)
        if len(self.data) >= 8:
            self.firmware_version = self.data[0:2]
            self.type = ReaderType(self.data[2])
            self.supports_6b = (self.data[3] & 0b01) > 0
            self.supports_6c = (self.data[3] & 0b10) > 0
            dmaxfre = self.data[4]
            dminfre = self.data[5]
            self.max_frequency = dmaxfre & 0b00111111
            self.min_frequency = dminfre & 0b00111111
            self.frequency_band = ReaderFrequencyBand(((dmaxfre & 0b11000000) >> 4) + ((dminfre & 0b11000000) >> 6))
            self.power = self.data[6]
            self.scan_time = self.data[7]
        else:
            raise ValueError('Data must be at least 8 characters')

    def get_regional_frequency(self, fnum):
        if self.frequency_band is ReaderFrequencyBand.EU:
            return 865.1 + fnum * 0.2
        elif self.frequency_band is ReaderFrequencyBand.China2:
            return 920.125 + fnum * 0.25
        elif self.frequency_band is ReaderFrequencyBand.US:
            return 902.75 + fnum * 0.5
        elif self.frequency_band is ReaderFrequencyBand.Korea:
            return 917.1 + fnum * 0.2
        elif self.frequency_band is ReaderFrequencyBand.Ukraine:
            return 868.0 + fnum * 0.1
        elif self.frequency_band is ReaderFrequencyBand.Peru:
            return 916.2 + fnum * 0.9
        elif self.frequency_band is ReaderFrequencyBand.China1:
            return 840.125 + fnum * 0.25
        elif self.frequency_band is ReaderFrequencyBand.EU3:
            return 865.7 + fnum * 0.6
        elif self.frequency_band is ReaderFrequencyBand.US3:
            return 902 + fnum * 0.5
        elif self.frequency_band is ReaderFrequencyBand.Taiwan:
            return 922.25 + fnum * 0.5
        else:
            return fnum

    def get_min_frequency(self):
        return self.get_regional_frequency(self.min_frequency)

    def get_max_frequency(self):
        return self.get_regional_frequency(self.max_frequency)


class TagData(object):

    def __init__(self, resp_bytes, prefix_bytes=0, suffix_bytes=0):
        self.prefix_bytes = prefix_bytes
        self.suffix_bytes = suffix_bytes
        self.data = resp_bytes
        self.num_tags = resp_bytes[0]

    def get_tag_data(self):
        n = 0
        pointer = 1
        while n < self.num_tags:
            tag_len = int(self.data[pointer])
            tag_data_start = pointer + 1
            tag_main_start = tag_data_start + self.prefix_bytes
            tag_main_end = tag_main_start + tag_len
            next_tag_start = tag_main_end + self.suffix_bytes
            yield (self.data[tag_data_start:tag_main_start], self.data[tag_main_start:tag_main_end],
                   self.data[tag_main_end:next_tag_start])
            pointer = next_tag_start
            n += 1


class Tag(object):

    def __init__(self, epc, antenna_num=1, rssi=None):
        self.epc = epc
        self.antenna_num = antenna_num
        self.rssi = rssi


# --------------------------------------------------------

get_reader_info = ReaderCommand(CF_GET_READER_INFO)
get_inventory_288 = G2InventoryCommand(q_value=4, antenna=0x80)
get_inventory_uhfreader18 = ReaderCommand(G2_TAG_INVENTORY)

# Todo: change device port from 'COM3' to '/dev/ttyUSB0' if use Raspberry pi or any Linux OS
transport = SerialTransport(device='/dev/ttyUSB0')
# transport = TcpTransport(reader_addr='192.168.0.250', reader_port=27011)
# transport = TcpTransport(reader_addr='192.168.1.190', reader_port=6000)
runner = CommandRunner(transport)

# transport.write(get_inventory_288.serialize())
transport.write(get_inventory_uhfreader18.serialize())
inventory_status = None
while inventory_status is None or inventory_status == G2_TAG_INVENTORY_STATUS_MORE_FRAMES:
    # g2_response = G2InventoryResponseFrame288(transport.read_frame())
    g2_response = G2InventoryResponseFrame18(transport.read_frame())
    inventory_status = g2_response.result_status
    for tag in g2_response.get_tag():
        print('Antenna %d: EPC %s, RSSI %s' % (tag.antenna_num, tag.epc.hex(), tag.rssi))
        try:
            # cur = db.cursor()
            # sendData = 'INSERT INTO tags VALUES(%s,%s,%s)'
            # tagSend = tag.epc.hex()
            # ts = [(tagSend,"my","jsh")]
            # cur.executemany(sendData, ts)
            # db.commit()

            # database UPDATE query for retriving data
            cur = db.cursor()
            # sendData = 'INSERT INTO tags VALUES(%s,%s,%s)'
            tagValue = tag.epc.hex()
            tagSend = str.upper(tagValue)
            print(f"The sent Tag Id is {tagSend}")

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            updateData = 'UPDATE tags SET status="Tight", updatedAt=(%s)  WHERE tagId=(%s)'
            ts = [(timestamp, tagSend,)]
            cur.executemany(updateData, ts)
            db.commit()
            print("data has been updated!")
        except mysql.connector.Error as error:
            print("Failed to update table record: {}".format(error))

transport.close()
