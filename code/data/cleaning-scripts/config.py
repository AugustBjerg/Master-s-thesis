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
    'Vessel External Conditions Wave Period (Provider S)',
    'Vessel External Conditions Sea Water Temperature (Provider S)',
    'Vessel External Conditions Northward Wind Velocity (Provider S)',
    'Vessel External Conditions Eastward Wind Velocity (Provider S)'
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

NO_REPETITION_SENSOR_VARIABLES = [
    'Main Engine Scavenging Air Pressure',
    'Main Engine Fuel Oil Inlet Mass Flow',
    'Vessel Propeller Shaft Torque',
    'Vessel Propeller Shaft Thrust Force',
    'Vessel Propeller Shaft Mechanical Power',
]

# Low pass filter configuration
SENSOR_SPIKE_THRESHOLDS = {
    'Main Engine Rotational Speed': 5.0,
    'Vessel External Conditions Wind Relative Speed': 5.0,
    'Vessel External Conditions Wind Relative Angle': 5.0,
    'Vessel Hull Over Ground Speed': 5.0,
    'Vessel Hull Heading Turn Rate': 5.0,
    'Vessel Hull Heading True Angle': 5.0,
    'Main Engine Turbocharger Rotational Speed': 5.0,
    'Vessel Hull Through Water Longitudinal Speed': 5.0,
    'Main Engine Fuel Oil Inlet Mass Flow': 5.0,
    'Vessel Propeller Shaft Mechanical Power': 5.0,
    'Vessel Propeller Shaft Rotational Speed': 5.0,
    'Vessel Propeller Shaft Torque': 5.0,
    'Vessel Propeller Shaft Thrust Force': 5.0,
    'Main Engine Fuel Load %': 5.0,
    'Main Engine Scavenging Air Pressure': 5.0,
}
LOW_PASS_WINDOW_SIZE_SECONDS = 20  # 20 observations × 15s = 5 minutes
LOW_PASS_MIN_PERIODS = 10
MAX_CONSECUTIVE_SPIKES = 10  # if more than 10 consecutive observations are marked as spikes for a variable, we will not impute them and instead reject them.

# Configurations for the rolling average filtering (steady state identification)
ROLLING_STD_THRESHOLDS = {
    'Vessel Hull Through Water Longitudinal Speed': 0.388,  # 0.1 m/s ≈ 0.194 knots * 2 (double of DNV recommendation)
    'Vessel Hull Over Ground Speed': 0.388,  # 0.1 m/s ≈ 0.194 knots * 2 (double of DNV recommendation)
    'Vessel Hull Heading True Angle': 2.0  # 2 degrees
}
ROLLING_STD_WINDOW_SIZE = 120 # 120 observations corresponds to 30 minutes at a 15-second sampling interval
ROLLING_STD_MIN_PERIODS = 60 # require at least 60 observations (15 minutes) to calculate a rolling std, to avoid flagging too many observations at the start of segments

SPEED_THROUGH_WATER_THRESHOLD = 4 # knots. Observations with a speed through water below this threshold will be removed, as they are likely to correspond to maneuvering or other unsteady operations that are not of interest for the analysis.

THRESHOLD_FACTOR = 0.5

# Minimum number of timestamps a continuous segment must have to be retained
MIN_SEGMENT_LENGTH_SECONDS = 7200 # 2 hours

DROP_TRANDUCER_DEPTH = True

# The highest tolerated deviation between calculated shaft power (from rpm and torque) and measured shaft power (from torquemeter) in percentage. Observations with a higher deviation will be replaced with NaN.
SHAFT_POWER_MAX_DEVIATION = 0.02

# The highest tolerated deviation between calculated shaft revolutions delta (from rpm) and measured shaft revolutions delta (from cumulative shaft revolutions) in percentage. Observations with a higher deviation will be replaced with NaN.
SHAFT_REVOLUTIONS_MAX_DEVIATION = 0.05

# Aggregation window configuration
WINDOW_LENGTH = "5min"

MIN_WINDOW_COVERAGE = 0.9

WINDOW_SIDE = "left"

WINDOW_LABEL = "left"

SENSOR_DATA_AGGREGATION_METHODS = {
    "Vessel Hull Over Ground Speed (knots)": "mean",
    "Vessel Hull Through Water Longitudinal Speed (knots)": "mean",
    "Vessel External Conditions Wind Relative Speed (knots)": "mean",
    "Vessel External Conditions Wind Relative Angle (degrees)": "", # left empty because specific function for cumulative counters is added later
    "Vessel Hull Heading True Angle (degrees)": "", # left empty because specific function for cumulative counters is added later
    "Vessel Hull Heading Turn Rate (deg/min)": "mean",
    "Main Engine Turbocharger Rotational Speed (rpm)": "mean",
    "Main Engine Scavenging Air Pressure (bar)": "mean",
    "Main Engine Fuel Load % (%)": "mean",
    "Main Engine Rotational Speed (rpm)": "mean",
    "Vessel Propeller Shaft Torque (N*m)": "mean",
    "Vessel Propeller Shaft Mechanical Power (KW)": "mean",
    "Main Engine Fuel Oil Inlet Mass Flow (kg/hr)": "mean",
    "Vessel Propeller Shaft Mechanical Energy (KWh)": "", # left empty because specific function for cumulative counters is added later
    "Vessel Propeller Shaft Thrust Force (KN)": "mean",
    "Vessel Propeller Shaft Rotational Speed (rpm)": "mean",
    "Vessel Propeller Shaft Revolutions (cumulative) (revs)": "", # left empty because specific function for cumulative counters is added later
    "Vessel External Conditions Northward Sea Water Velocity (Provider MB)": "mean", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Vessel External Conditions Wave Significant Height (Provider MB)": "mean", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Vessel External Conditions Eastward Wind Velocity (Provider S)": "mean", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Vessel External Conditions Wind True Angle (Provider MB)": "", # Left empty for later replacement. Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Vessel External Conditions Swell Significant Height (Provider MB)": "mean", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Vessel External Conditions Sea Water Temperature (Provider S)": "mean", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Vessel External Conditions Eastward Sea Water Velocity (Provider MB)": "mean", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Vessel External Conditions Wave Period (Provider S)": "mean", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Vessel External Conditions Wind True Speed (Provider MB)": "mean", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Vessel External Conditions Northward Sea Water Velocity (Provider S)": "mean", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Vessel External Conditions Wave Significant Height (Provider S)": "mean", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Vessel External Conditions Northward Wind Velocity (Provider S)": "mean", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Sea Temperature Dropout": "sum", # Somewhat arbitrary since modelling is done at lower sampling interval than the original data (so it is forward filled instead)
    "Calculated Shaft Power": "mean", 
    "Imputed Spike in Main Engine Rotational Speed": "sum",
    "Imputed Spike in Vessel External Conditions Wind Relative Speed": "sum",
    "Imputed Spike in Vessel External Conditions Wind Relative Angle": "sum",
    "Imputed Spike in Vessel Hull Over Ground Speed": "sum",
    "Imputed Spike in Vessel Hull Heading Turn Rate": "sum",
    "Imputed Spike in Vessel Hull Heading True Angle": "sum",
    "Imputed Spike in Main Engine Turbocharger Rotational Speed": "sum",
    "Imputed Spike in Vessel Hull Through Water Longitudinal Speed": "sum",
    "Imputed Spike in Main Engine Fuel Oil Inlet Mass Flow": "sum",
    "Imputed Spike in Vessel Propeller Shaft Mechanical Power": "sum",
    "Imputed Spike in Vessel Propeller Shaft Rotational Speed": "sum",
    "Imputed Spike in Vessel Propeller Shaft Torque": "sum",
    "Imputed Spike in Vessel Propeller Shaft Thrust Force": "sum",
    "Imputed Spike in Main Engine Fuel Load %": "sum",
    "Imputed Spike in Main Engine Scavenging Air Pressure": "sum",
}

ANGLE_COLUMNS = [
    "Vessel External Conditions Wind Relative Angle (degrees)",
    "Vessel Hull Heading True Angle (degrees)",
    "Vessel External Conditions Wind True Angle (Provider MB)",
]

CUMULATIVE_COLS = [
    "Vessel Propeller Shaft Revolutions (cumulative) (revs)",
    "Vessel Propeller Shaft Mechanical Energy (KWh)",
]
