import time
import mysql.connector
from datetime import datetime
from pymodbus.client import ModbusTcpClient
import struct

client = ModbusTcpClient('192.168.255.2', port = 502)
client.connect()

high_reg = []
low_reg = []
combined = []
float_value = []
#while True:
hh = client.read_input_registers(address=1000, count =18)
for i in range (0, 17, 2):
    high_reg.append(hh.registers[i+1])
    low_reg.append(hh.registers[i])
for i in range (0,9):
    combined.append((high_reg[i]<<16)|low_reg[i])
    
for i in range (0,9):
    float_value.append(struct.unpack('>f', struct.pack('>I', combined[i]))[0])
    
ff = client.read_input_registers(address=1068, count = 2)
ff_h_reg = ff.registers[1]
ff_l_reg = ff.registers[0]
ff_combined = ff_h_reg << 16 | ff_l_reg
Freq = round((struct.unpack('>f', struct.pack('>I', ff_combined))[0]),2)

V_1 = round(float_value[0],3) #34353-34354x
I_1 = round(float_value[1],3) #34355-34356
kW_1 = round(float_value[2],3) #34357-34358
kvar_1 = round(float_value[3],3) #34359-34360
kVA_1 = round(float_value[4],3) #34361-34362
PF_1 = round(float_value[5],3) #34363-34364
kWh_1 = round(float_value[6],3) #34365-34366
kvarh_1 = round(float_value[7],3) #34367-34368
kVAh_1 = round(float_value[8],3) #34369-34370
print("Volt: " + str(V_1) + " Volt")
print("Ampere: " + str(I_1) + " A")
print("kW: " + str(kW_1) + " Watt")
print("kvar: " + str(kvar_1) + " VAR")
print("kVA: " + str(kVA_1) + " VA")
print("PF: " + str(PF_1))
print("kWh: " + str(kWh_1) + " Watt Hour")
print("kvarh: " + str(kvarh_1) + " VAR Hour")
print("kVAh: " + str(kVAh_1) + " VA Hour")
print("Frequency: " + str(Freq)+ " Hz")
print("")

conn = mysql.connector.connect(
    user ='satsindo', password = 'satsindo', host = '192.168.100.90', database = 'integration_database'
)
cursor = conn.cursor()

Year = datetime.now().year
Month = datetime.now().month
Date = datetime.now().day
Time = time.strftime("%H:%M:%S", time.localtime())
Voltage = V_1
Current = I_1
Power = kW_1*1000
PF = PF_1
Frequency = Freq

sql = (
    "INSERT INTO history_data (No_Group, Year, Month, Date, Time, Voltage, Current, Power, PF, Frequency)"
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)
data = (3, Year, Month, Date, Time, Voltage, Current, Power, PF, Frequency)

cursor.execute(sql, data)
conn.commit()