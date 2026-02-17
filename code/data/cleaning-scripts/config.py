EXPECTED_SENSOR_OBSERVATIONS = 41079968

NOON_REPORT_QIDS = {
    'Slip' : "0::0::0::0_0::0::0::0::0_0::0::0::0_1" ,
    'Fwd Draft' : "0::0::0::0_0::0::0::0::0_0::0::0::0_2" ,
    'Mid Draft' : "0::0::0::0_0::0::0::0::0_0::0::0::0_3" ,
    'Aft Draft' : "0::0::0::0_0::0::0::0::0_0::0::0::0_4" ,
    'Displacement' : "0::0::0::0_0::0::0::0::0_0::0::0::0_5" ,
    'Air Temp' : "0::0::0::0_0::0::0::0::0_0::0::0::0_6" ,
    'Bar Pressure' : "0::0::0::0_0::0::0::0::0_0::0::0::0_7" ,
    'Sea State' : "0::0::0::0_0::0::0::0::0_0::0::0::0_8" ,
    'Wind Force' : "0::0::0::0_0::0::0::0::0_0::0::0::0_9" ,
    'Sea Temp' : "0::0::0::0_0::0::0::0::0_0::0::0::0_10" ,
    'Sea Direction' : "0::0::0::0_0::0::0::0::0_0::0::0::0_11" ,
    'Wind Direction' : "0::0::0::0_0::0::0::0::0_0::0::0::0_12" ,
    'Consumption for Propulsion' : "0::0::0::0_0::0::0::0::0_0::0::0::0_13" ,
    'Fuel' : "0::0::0::0_0::0::0::0::0_0::0::0::0_14"
    }

NOON_REPORT_UNITS = {
    'Slip' : "%" ,
    'Fwd Draft' : "m" ,
    'Mid Draft' : "m" ,
    'Aft Draft' : "m" ,
    'Displacement' : "t" ,
    'Air Temp' : "°C" ,
    'Bar Pressure' : "Millibars" ,
    'Sea State' : "Douglas scale" ,
    'Wind Force' : "Beaufort" ,
    'Sea Temp' : "°C" ,
    'Sea Direction' : "°" ,
    'Wind Direction' : "°" ,
    'Consumption for Propulsion' : "MT/day" ,
    'Fuel' : "categorical"
    }

INCLUDED_NOON_REPORT_QIDS = NOON_REPORT_QIDS = [
    "0::0::0::0_0::0::0::0::0_0::0::0::0_2" , # Fwd Draft
    "0::0::0::0_0::0::0::0::0_0::0::0::0_3" ,
    "0::0::0::0_0::0::0::0::0_0::0::0::0_4" ,
    ]

INTENDED_SAMPLING_INTERVALS_SECONDS = {
    # Noon Reports - measured every 24h
    "0::0::0::0_0::0::0::0::0_0::0::0::0_1" : 86400, # Slip
    "0::0::0::0_0::0::0::0::0_0::0::0::0_2" : 86400, # Fwd Draft
    "0::0::0::0_0::0::0::0::0_0::0::0::0_3" : 86400, # Mid Draft
    "0::0::0::0_0::0::0::0::0_0::0::0::0_4" : 86400, # Aft Draft
    "0::0::0::0_0::0::0::0::0_0::0::0::0_5" : 86400, # Displacement
    "0::0::0::0_0::0::0::0::0_0::0::0::0_6" : 86400, # Air Temp
    "0::0::0::0_0::0::0::0::0_0::0::0::0_7" : 86400, # Bar Pressure
    "0::0::0::0_0::0::0::0::0_0::0::0::0_8" : 86400, # Sea State
    "0::0::0::0_0::0::0::0::0_0::0::0::0_9" : 86400, # Wind Force
    "0::0::0::0_0::0::0::0::0_0::0::0::0_10" : 86400, # Sea Temp
    "0::0::0::0_0::0::0::0::0_0::0::0::0_11" : 86400, # Sea Direction
    "0::0::0::0_0::0::0::0::0_0::0::0::0_12" : 86400, # Wind Direction
    "0::0::0::0_0::0::0::0::0_0::0::0::0_13" : 86400, # Consumption for Propulsion
    "0::0::0::0_0::0::0::0::0_0::0::0::0_14" : 86400, # Fuel
    
    # Weather data from Provider MB and Provider S - measured every hour
    "4::0::4::0_1::1::0::7::0_45::0::1::0_8" : 3600, # Vessel External Conditions Wave Significant Height (Provider MB)
    "4::0::4::0_1::1::0::7::0_2::0::15::21_8" : 3600, # Vessel External Conditions Wind True Angle (Provider MB)
    "4::0::4::0_1::1::0::7::0_45::0::2::0_8" : 3600, # Vessel External Conditions Swell Significant Height (Provider MB)
    "4::0::4::0_1::1::0::7::0_1::0::4::21_8" : 3600, # Vessel External Conditions Wind True Speed (Provider MB)
    "4::0::4::0_1::1::0::7::0_56::0::3::0_8" : 3600, # Vessel External Conditions Eastward Sea Water Velocity (Provider MB)
    "4::0::4::0_1::1::0::7::0_56::0::4::0_8" : 3600, # Vessel External Conditions Northward Sea Water Velocity (Provider MB)
    "4::0::8::0_1::1::0::7::0_45::0::1::0_8" : 3600, # Vessel External Conditions Wave Significant Height (Provider S)
    "4::0::8::0_1::1::0::7::0_2::0::18::21_8" : 3600, # Vessel External Conditions Wave True Angle (Provider S)
    "4::0::8::0_1::1::0::7::0_40::0::2::0_8" : 3600, # Vessel External Conditions Wave Period (Provider S)
    "4::0::8::0_1::1::0::7::0_4::0::12::0_8" : 3600, # Vessel External Conditions Sea Water Temperature (Provider S)
    "4::0::8::0_1::1::0::7::0_56::0::6::0_8" : 3600, # Vessel External Conditions Northward Wind Velocity (Provider S)
    "4::0::8::0_1::1::0::7::0_56::0::5::0_8" : 3600, # Vessel External Conditions Eastward Wind Velocity (Provider S)
    "4::0::8::0_1::1::0::7::0_56::0::3::0_8" : 3600, # Vessel External Conditions Eastward Sea Water Velocity (Provider S)
    "4::0::8::0_1::1::0::7::0_56::0::4::0_8" : 3600, # Vessel External Conditions Northward Sea Water Velocity (Provider S)
    
    # Sensor data - measured every 15 seconds
    "3::0::1::0_1::1::0::2::0_11::0::2::0_8" : 15, # Vessel Hull Aft Draft (Control Alarm Monitoring System)
    "3::0::1::0_1::1::0::2::0_11::0::1::0_8" : 15, # Vessel Hull Fore Draft (Control Alarm Monitoring System)
    "3::0::1::0_1::2::0::8::0_1::0::6::0_8" : 15, # Main Engine Rotational Speed (Control Alarm Monitoring System)
    "3::0::1::0_1::1::0::2::0_11::0::3::0_8" : 15, # Vessel Hull MidP Draft (Control Alarm Monitoring System)
    "3::0::1::0_1::1::0::2::0_11::0::4::0_8" : 15, # Vessel Hull MidS Draft (Control Alarm Monitoring System)
    "2::0::1::0_1::1::0::7::0_1::0::4::22_8" : 15, # Vessel External Conditions Wind Relative Speed (Instrument Anemometer)
    "2::0::1::0_1::1::0::7::0_2::0::15::22_8" : 15, # Vessel External Conditions Wind Relative Angle (Instrument Anemometer)
    "2::0::4::0_1::1::0::2::0_37::0::2::0_8" : 15, # Vessel Hull Relative To Transducer Water Depth (Instrument Echosounder)
    "2::0::6::1_1::1::0::2::0_1::0::1::0_8" : 15, # Vessel Hull Over Ground Speed (Instrument GPS 1)
    "2::0::5::0_1::1::0::2::0_6::0::1::0_8" : 15, # Vessel Hull Heading Turn Rate (Instrument Gyrocompass)
    "2::0::5::0_1::1::0::2::0_2::0::8::21_8" : 15, # Vessel Hull Heading True Angle (Instrument Gyrocompass)
    "2::0::25::0_1::2::0::3::0_1::0::6::0_8" : 15, # Main Engine Turbocharger Rotational Speed (Instrument RPM Indicator)
    "2::0::7::0_1::1::0::2::0_1::0::5::11_8" : 15, # Vessel Hull Through Water Longitudinal Speed (Instrument Speedlog)
    "2::0::11::0_1::2::0::8::0_22::0::1::1_8" : 15, # Main Engine Fuel Oil Inlet Mass Flow (Instrument Torquemeter)
    "2::0::11::0_1::1::0::3::0_14::0::1::0_8" : 15, # Vessel Propeller Shaft Mechanical Power (Instrument Torquemeter)
    "2::0::11::0_1::1::0::3::0_1::0::6::0_8" : 15, # Vessel Propeller Shaft Rotational Speed (Instrument Torquemeter)
    "2::0::11::0_1::1::0::3::0_12::0::2::0_8" : 15, # Vessel Propeller Shaft Torque (Instrument Torquemeter)
    "2::0::11::0_1::1::0::3::0_17::0::1::0_8" : 15, # Vessel Propeller Shaft Thrust Force (Instrument Torquemeter)
    "2::0::11::0_1::1::0::3::0_15::0::1::0_8" : 15, # Vessel Propeller Shaft Mechanical Energy (Instrument Torquemeter)
    "2::0::11::0_1::1::0::3::0_12::0::1::0_8" : 15, # Vessel Propeller Shaft Revolutions (cumulative) (Instrument Torquemeter)
    "1::0::25::0_1::2::0::8::0_20::0::1::0_8" : 15, # Main Engine Fuel Load % (Transducer Fuel Load)
    "1::0::14::0_1::2::0::8::0_3::0::3::0_8" : 15, # Main Engine Scavenging Air Pressure (Transducer Pressure)
    "1::0::15::0_1::2::0::3::0_1::0::6::0_8" : 15, # Main Engine Turbocharger Rotational Speed (Transducer RPM)
}

THRESHOLD_FACTOR = 0.5

# Minimum number of timestamps a continuous segment must have to be retained
MIN_SEGMENT_LENGTH_SECONDS = 7200 # 2 hours

DROP_TRANDUCER_DEPTH = True

# The highest tolerated deviation between calculated shaft power (from rpm and torque) and measured shaft power (from torquemeter) in percentage. Observations with a higher deviation will be replaced with NaN.
SHAFT_POWER_MAX_DEVIATION = 0.02