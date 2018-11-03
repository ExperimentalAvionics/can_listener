#!/usr/bin/python3
#

import can
import time
import os
import sqlite3

#get the database ready
memdb = sqlite3.connect('/memdb/memdb.db')
cursor = memdb.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS  messages(id INTEGER PRIMARY KEY AUTOINCREMENT, CANid INT, Param_Text varchar(50), Param_Value REAL,  msg TEXT, timestamp REAL)
''')
memdb.commit()
cursor.execute('''
    CREATE UNIQUE INDEX IF NOT EXISTS data_idx ON messages(CANid, Param_Text)
''')
memdb.commit()


cursor.execute('''PRAGMA journal_mode=WAL''')

# populate table with known parameters

#Msg. 25 Time broadcast

#Msg. 40 from Module A (Air pressure and stuff)
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (40, "Airspeed", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (40, "Altitude", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (40, "VerticalSpeed", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (40, "AoA", 0, time.time() ))
#Msg. 42 Outside Air Temperature, and Humidity
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (40, "OAT", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (40, "Humidity", 0, time.time() ))
#Msg. 46 QNH
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (46, "QNH", 0, time.time() ))
#Msg. 50 Engine stuff
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (50, "RPM", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (50, "FuelPressure", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (50, "FuelFlow", 0, time.time() ))
#Msg. 72 Giro data
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (72, "Heading", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (72, "Roll", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (72, "Pitch", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (72, "TurnRate", 0, time.time() ))
#Msg. 73 Acceleration
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (73, "AccX", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (73, "AccY", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (73, "AccZ", 0, time.time() ))
#Msg. 80 Fuel Levels
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (80, "FuelTank1", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (80, "FuelTank2", 0, time.time() ))
#Msg. 81 Oil Temperature and pressure
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (81, "OilTemperature", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (81, "OilPressure", 0, time.time() ))
#Msg. 82 EGT 1&2 + CHT 1&2
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (82, "EGT1", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (82, "EGT2", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (82, "CHT1", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (82, "CHT2", 0, time.time() ))
#Msg. 83 EGT 3&4 + CHT 3&4
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (83, "EGT3", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (83, "EGT4", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (83, "CHT3", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (83, "CHT4", 0, time.time() ))
#Msg. 84 EGT 5&6 + CHT 5&6
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (84, "EGT5", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (84, "EGT6", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (84, "CHT5", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (84, "CHT6", 0, time.time() ))
#Msg. 85 Electric stuff Voltage and Current
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (85, "Volts", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (85, "Amps", 0, time.time() ))
#Msg. 99 GPS Lat/Lon
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (99, "GPS_Lat", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (99, "GPS_Lon", 0, time.time() ))
#Msg. 100 GPS GS, ALT, Tracking True and Magnetic
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (100, "GPS_GS", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (100, "GPS_Alt", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (100, "GPS_TRK_T", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (100, "GPS_TRK_M", 0, time.time() ))
#Msg. 112 Engine Time and Flight Switch time
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (112, "EngineTimeTacho", 0, time.time() ))
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (112, "EngineTimeClock", 0, time.time() ))
#Msg. 114 Airswitch (Flight time)
cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, Param_Value, timestamp) VALUES(?,?,?,?)''', (120, "Airswitch", 0, time.time() ))


memdb.commit()


#confdb = sqlite3.connect('config.db')
#confcur = confdb.cursor()

print('Bring up CAN0....')
#os.system("sudo /sbin/ip link set can0 up type can bitrate 500000")
time.sleep(0.1)	

try:
	bus = can.interface.Bus(channel='can0', bustype='socketcan_native')
except OSError:
	print('Cannot find CAN board.')
	exit()

print('Ready')

try:
	while True:
# maybe use  
#               for msg in bus:
#                   print(msg.data)
# instead
		message = bus.recv()	# Wait until a message is received.
		s=''
		c = '{0:f} {1:x} {2:x} '.format(message.timestamp, message.arbitration_id, message.dlc)
#		confcur.execute('''SELECT structure FROM CANmsg WHERE CANid=?''', (message.arbitration_id))
#		msg_config = confcur.fetchone()
#		structure = msg_config[0]
		CANid=message.arbitration_id
		if CANid == 40:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8), message.timestamp, CANid, "Airspeed"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[2])|(message.data[3]<<8)|(message.data[4]<<16), message.timestamp, CANid, "Altitude"))
			VerticalSpeed = (message.data[5])|(message.data[6]<<8)
			if VerticalSpeed > 32768:
				VerticalSpeed = VerticalSpeed - 65536
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', (VerticalSpeed, message.timestamp, CANid, "VerticalSpeed"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ( message.data[7], 	message.timestamp, CANid, "AoA"))
			memdb.commit()
		elif CANid == 42:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', (message.data[0], message.timestamp, CANid, "OAT"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', (message.data[1], message.timestamp, CANid, "Humidity"))
			memdb.commit()
		elif CANid == 46:
                        cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8), message.timestamp, CANid, "QNH"))
                        memdb.commit()
		elif CANid == 50:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8), message.timestamp, CANid, "RPM"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[2])|(message.data[3]<<8), message.timestamp, CANid, "FuelPressure"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[4])|(message.data[5]<<8), message.timestamp, CANid, "FuelFlow"))
			memdb.commit()
		elif CANid == 72:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8), message.timestamp, CANid, "Heading"))
			Roll = (message.data[2])|(message.data[3]<<8)
			if Roll > 32768:
				Roll = Roll - 65536
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', (Roll, message.timestamp, CANid, "Roll"))
			Pitch = (message.data[4])|(message.data[5]<<8)
			if Pitch > 32768:
				Pitch = Pitch - 65536
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', (Pitch, message.timestamp, CANid, "Pitch"))
			TurnRate = (message.data[6])|(message.data[7]<<8)
			if TurnRate > 32768:
				TurnRate = TurnRate - 65536
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', (TurnRate, message.timestamp, CANid, "TurnRate"))
			memdb.commit()
		elif CANid == 73:
			AccX = (message.data[0])|(message.data[1]<<8)
			if AccX > 32768:
				AccX = AccX - 65536
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', (AccX, message.timestamp, CANid, "AccX"))
			AccY = (message.data[2])|(message.data[3]<<8)
			if AccY > 32768:
				AccY = AccY - 65536
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', (AccY, message.timestamp, CANid, "AccY"))
			AccZ = (message.data[4])|(message.data[5]<<8)
			if AccZ > 32768:
				AccZ = AccZ - 65536
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', (AccZ, message.timestamp, CANid, "AccZ"))
			memdb.commit()
		elif CANid == 80:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8), message.timestamp, CANid, "FuelTank1"))
#			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[2])|(message.data[3]<<8), message.timestamp, CANid, "FuelTank2"))
			memdb.commit()
		elif CANid == 81:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8), message.timestamp, CANid, "OilTemperature"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[2])|(message.data[3]<<8), message.timestamp, CANid, "OilPressure"))
			memdb.commit()
		elif CANid == 82:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8), message.timestamp, CANid, "EGT1"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[2])|(message.data[3]<<8), message.timestamp, CANid, "EGT2"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[4])|(message.data[5]<<8), message.timestamp, CANid, "CHT1"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[6])|(message.data[7]<<8), message.timestamp, CANid, "CHT2"))
			memdb.commit()
		elif CANid == 83:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8), message.timestamp, CANid, "EGT3"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[2])|(message.data[3]<<8), message.timestamp, CANid, "EGT4"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[4])|(message.data[5]<<8), message.timestamp, CANid, "CHT3"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[6])|(message.data[7]<<8), message.timestamp, CANid, "CHT4"))
			memdb.commit()
		elif CANid == 84:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8), message.timestamp, CANid, "EGT5"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[2])|(message.data[3]<<8), message.timestamp, CANid, "EGT6"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[4])|(message.data[5]<<8), message.timestamp, CANid, "CHT5"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[6])|(message.data[7]<<8), message.timestamp, CANid, "CHT6"))
			memdb.commit()
		elif CANid == 85:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8), message.timestamp, CANid, "Volts"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[2])|(message.data[3]<<8), message.timestamp, CANid, "Amps"))
			memdb.commit()
		elif CANid == 99:
			Lat = (message.data[0])|(message.data[1]<<8)|(message.data[2]<<16)|(message.data[3]<<24)
			if Lat > 2147483648:
				Lat = Lat - 4294967295
			Lon = (message.data[4])|(message.data[5]<<8)|(message.data[6]<<16)|(message.data[7]<<24)
			if Lon > 2147483648:
				Lon = Lon - 4294967295
			Lat = Lat/1000000
			Lon = Lon/1000000
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', (Lat, message.timestamp, CANid, "GPS_Lat"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', (Lon, message.timestamp, CANid, "GPS_Lon"))
		elif CANid == 100:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8), message.timestamp, CANid, "GPS_GS"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[2])|(message.data[3]<<8), message.timestamp, CANid, "GPS_Alt"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[4])|(message.data[5]<<8), message.timestamp, CANid, "GPS_TRK_T"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[6])|(message.data[7]<<8), message.timestamp, CANid, "GPS_TRK_M"))
			memdb.commit()
		elif CANid == 112:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8)|(message.data[2]<<16)|(message.data[3]<<24), message.timestamp, CANid, "EngineTimeTacho"))
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[4])|(message.data[5]<<8)|(message.data[6]<<16)|(message.data[7]<<24), message.timestamp, CANid, "EngineTimeClock"))
			memdb.commit()
		elif CANid == 114:
			cursor.execute('''UPDATE messages SET Param_Value=?, timestamp=? WHERE CANid=? and Param_Text=?''', ((message.data[0])|(message.data[1]<<8)|(message.data[2])|(message.data[3]<<8), message.timestamp, CANid, "Airswitch"))
			memdb.commit()
		else:
			for i in range(message.dlc ):
				s +=  '{0:x} '.format(message.data[i])
			cursor.execute('''INSERT OR REPLACE  INTO messages(CANid, Param_Text, msg, timestamp) VALUES(?,?,?,?)''', (CANid, "unknown", s, message.timestamp))
			memdb.commit()


#		for i in range(message.dlc ):
#			s +=  '{0:x} '.format(message.data[i])
#		print(' {}'.format(c+s))

except KeyboardInterrupt:
	print('\n\rKeyboard interrupt')
