# CAN Listener
This is Python script that listens to the CAN-Bus and writes the data into SQLite database located in memory (virtual disk). 
All other software components running on the same host (Raspberry Pi) will be reading data from this SQLite database when required.

For details visit http://experimentalavionics.com/can-bus-data-logger/


## Release Notes: ##

### 2021-10-20 ###
* Fixed the bug registeing Outside Air Temperature (OAT) incorrectly.

### 2021-07-25 ###
* Added support for two electric current sensors - Alternator Ampermeter (AmpsAlternator) and  Battery Ampermeter (AmpsBattery)

### 2021-06-17 ###
* Bug fix: Incorrect reporting of oil pressure and temperature
* Bux fix: added reporting of TimeSync events (CAN message 25)

