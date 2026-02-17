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

NAN_IMPUTATION_STRATEGIES = {
    # Noon Reports
    'Slip': 'forward_fill',
    'Fwd Draft': 'forward_fill',
    'Mid Draft': 'forward_fill',
    'Aft Draft': 'forward_fill',
    'Displacement': 'forward_fill',
    'Air Temp': 'forward_fill',
    'Bar Pressure': 'forward_fill',
    'Sea State': 'forward_fill',
    'Wind Force': 'forward_fill',
    'Sea Temp': 'forward_fill',
    'Sea Direction': 'forward_fill',
    'Wind Direction': 'forward_fill',
    'Consumption for Propulsion': 'forward_fill',
    'Fuel': 'forward_fill',
    
    # Weather data from Provider MB and Provider S
    'Vessel External Conditions Wave Significant Height (Provider MB)': 'interpolate',
    'Vessel External Conditions Wind True Angle (Provider MB)': 'interpolate',
    'Vessel External Conditions Swell Significant Height (Provider MB)': 'interpolate',
    'Vessel External Conditions Wind True Speed (Provider MB)': 'interpolate',
    'Vessel External Conditions Eastward Sea Water Velocity (Provider MB)': 'interpolate',
    'Vessel External Conditions Northward Sea Water Velocity (Provider MB)': 'interpolate',
    'Vessel External Conditions Wave Significant Height (Provider S)': 'interpolate',
    'Vessel External Conditions Wave True Angle (Provider S)': 'interpolate',
    'Vessel External Conditions Wave Period (Provider S)': 'interpolate',
    'Vessel External Conditions Sea Water Temperature (Provider S)': 'interpolate',
    'Vessel External Conditions Northward Wind Velocity (Provider S)': 'interpolate',
    'Vessel External Conditions Eastward Wind Velocity (Provider S)': 'interpolate',
    'Vessel External Conditions Eastward Sea Water Velocity (Provider S)': 'interpolate',
    'Vessel External Conditions Northward Sea Water Velocity (Provider S)': 'interpolate',
    
    # Sensor data
    'Vessel Hull Aft Draft': 'N/A', # column is only NaNs
    'Vessel Hull Fore Draft': 'N/A', # column is only NaNs
    'Main Engine Rotational Speed': 'interpolate',
    'Vessel Hull MidP Draft': 'N/A', # column is only NaNs
    'Vessel Hull MidS Draft': 'N/A', # column is only NaNs
    'Vessel External Conditions Wind Relative Speed': 'interpolate',
    'Vessel External Conditions Wind Relative Angle': 'interpolate',
    'Vessel Hull Relative To Transducer Water Depth': 'N/A', # column is dropped
    'Vessel Hull Over Ground Speed': 'interpolate',
    'Vessel Hull Heading Turn Rate': 'interpolate',
    'Vessel Hull Heading True Angle': 'interpolate',
    'Main Engine Turbocharger Rotational Speed': 'interpolate',
    'Vessel Hull Through Water Longitudinal Speed': 'interpolate',
    'Main Engine Fuel Oil Inlet Mass Flow': 'interpolate',
    'Vessel Propeller Shaft Mechanical Power': 'interpolate',
    'Vessel Propeller Shaft Rotational Speed': 'interpolate',
    'Vessel Propeller Shaft Torque': 'interpolate',
    'Vessel Propeller Shaft Thrust Force': 'interpolate',
    'Vessel Propeller Shaft Mechanical Energy': 'interpolate',
    'Vessel Propeller Shaft Revolutions': 'N/A', # column is dropped
    'Main Engine Fuel Load %': 'interpolate',
    'Main Engine Scavenging Air Pressure': 'interpolate',
}

REQUIRED_NOON_REPORT_VARIABLES = [
    'Slip',
    'Fwd Draft',
    'Mid Draft',
    'Aft Draft',
    'Displacement',
    'Air Temp',
    'Bar Pressure',
    'Sea State',
    'Wind Force',
    'Sea Temp',
    'Sea Direction',
    'Wind Direction',
    'Consumption for Propulsion',
    'Fuel',
]

REQUIRED_WEATHER_VARIABLES = [
    'Vessel External Conditions Wave Significant Height (Provider MB)',
    'Vessel External Conditions Wind True Angle (Provider MB)',
    'Vessel External Conditions Swell Significant Height (Provider MB)',
    'Vessel External Conditions Wind True Speed (Provider MB)',
    'Vessel External Conditions Eastward Sea Water Velocity (Provider MB)',
    'Vessel External Conditions Northward Sea Water Velocity (Provider MB)',
    'Vessel External Conditions Wave Significant Height (Provider S)',
    'Vessel External Conditions Wave True Angle (Provider S)',
    'Vessel External Conditions Wave Period (Provider S)',
    'Vessel External Conditions Sea Water Temperature (Provider S)',
    'Vessel External Conditions Northward Wind Velocity (Provider S)',
    'Vessel External Conditions Eastward Wind Velocity (Provider S)',
    'Vessel External Conditions Eastward Sea Water Velocity (Provider S)',
    'Vessel External Conditions Northward Sea Water Velocity (Provider S)',
]

REQUIRED_SENSOR_VARIABLES = [
    'Main Engine Rotational Speed',
    'Vessel External Conditions Wind Relative Speed',
    'Vessel External Conditions Wind Relative Angle',
    'Vessel Hull Over Ground Speed',
    'Vessel Hull Heading Turn Rate',
    'Vessel Hull Heading True Angle',
    'Main Engine Turbocharger Rotational Speed',
    'Vessel Hull Through Water Longitudinal Speed',
    'Main Engine Fuel Oil Inlet Mass Flow',
    'Vessel Propeller Shaft Mechanical Power',
    'Vessel Propeller Shaft Rotational Speed',
    'Vessel Propeller Shaft Torque',
    'Vessel Propeller Shaft Thrust Force',
    'Vessel Propeller Shaft Mechanical Energy',
    'Main Engine Fuel Load %',
    'Main Engine Scavenging Air Pressure',
]

# Configurations for the rolling average filtering (steady state identification)
ROLLING_STD_THRESHOLDS = {
    'Vessel Hull Through Water Longitudinal Speed': 0.194,  # 0.1 m/s ≈ 0.194 knots
    'Vessel Hull Over Ground Speed': 0.194,  # 0.1 m/s ≈ 0.194 knots
    'Vessel Hull Heading True Angle': 2.0  # 2 degrees
}
ROLLING_STD_WINDOW_SIZE = 120 # 120 observations corresponds to 30 minutes at a 15-second sampling interval
ROLLING_STD_MIN_PERIODS = 60 # require at least 60 observations (15 minutes) to calculate a rolling std, to avoid flagging too many observations at the start of segments

THRESHOLD_FACTOR = 0.5

# Minimum number of timestamps a continuous segment must have to be retained
MIN_SEGMENT_LENGTH_SECONDS = 7200 # 2 hours

DROP_TRANDUCER_DEPTH = True

# The highest tolerated deviation between calculated shaft power (from rpm and torque) and measured shaft power (from torquemeter) in percentage. Observations with a higher deviation will be replaced with NaN.
SHAFT_POWER_MAX_DEVIATION = 0.02

# The highest tolerated deviation between calculated shaft revolutions delta (from rpm) and measured shaft revolutions delta (from cumulative shaft revolutions) in percentage. Observations with a higher deviation will be replaced with NaN.
SHAFT_REVOLUTIONS_MAX_DEVIATION = 0.05