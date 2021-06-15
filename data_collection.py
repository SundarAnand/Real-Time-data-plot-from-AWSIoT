# Required libraries
#%matplotlib inline
import paho.mqtt.client as paho
import ssl
import json
import sys
import pandas as pd
import keyboard
from datetime import datetime
import boto3
import os
import matplotlib.pyplot as plt
import numpy as np
import math
from statistics import mean

# List to hold all the values
list_in_message=[]

# Getting activity and trial number
activity = input("Enter the activity: ")
trial = input("Enter the trial number: ")
plot = input("You want to plot (Y/N): ")

# Defining the window size
window_size = 5

# Stride size
stride_size = 1

# Defining a counter
count = 0

# Variables to be calculated
pitch_comp = 0
roll_comp = 0

# sampling rate in seconds
dt = 0.04 

# List to hold the offset values
pitch_acc_list = []
roll_acc_list = []

# S3 output location
currentTime = datetime.utcnow()
output_bucket = 'athletechrawdatacollection'
output_key = str(currentTime.year) + '-' + str(currentTime.month).zfill(2) + '-' + str(currentTime.day).zfill(2)
output_filename = activity + "_" + trial

# Function to upload the CSV file to S3
def upload_to_s3(df, upload_bucket, upload_folder, upload_filename, filetype='csv'):
    """
    :param df: pandas dataframe to upload to s3
    :param upload_bucket: str s3 bucket location
    :param upload_folder: str s3 folder location inside bucket ends with '/'
    :param upload_filename: filename t
    :param filetype: csv or feather
    :return:
    """
    if filetype == 'csv':
        filename = '/tmp/temp.csv'
        df.to_csv(filename, index=None, date_format='%Y-%m-%d %H:%M:%S')
    elif filetype == 'feather':
        filename = '/tmp/temp.feather'
        df.to_feather(filename)
    s3_client_connection = boto3.client('s3')
    s3_client_connection.upload_file(filename, upload_bucket, '%s/%s.%s' % (upload_folder, upload_filename, filetype))
    os.remove(filename)
    print('Combined file is saved in S3 server. Location: %s/%s/%s.%s\n' % (upload_bucket, upload_folder, upload_filename, filetype))

# Function to calculate pitch and roll
def pitch_roll_calculation(data):
    global pitch_acc_list, roll_acc_list, pitch_comp, roll_comp
    
    # Calculating denomitor for pitch accelerometer
    denom_pitch_acc = math.sqrt(data['acc_y']**2 + data["acc_z"]**2)

    # Calculating pitch accelerometer
    pitch_acc_temp = -math.atan2(data['acc_x'], denom_pitch_acc) * 180/math.pi
    
    # Removing offset from pitch acc by subtracting the mean of first 5 values
    if count < 6:
        pitch_acc_list.append(pitch_acc_temp)
    offset_pitch = mean(pitch_acc_list)
    pitch_acc_temp = pitch_acc_temp - offset_pitch

    # Calculating roll acc
    roll_acc_temp = math.atan2(data['acc_y'], data["acc_z"])*180/math.pi

    # Removing offset from roll acc by subtracting the mean of first 5 values
    if count < 6:
        roll_acc_list.append(roll_acc_temp)
    offset_roll = mean(roll_acc_list)
    roll_acc_temp = roll_acc_temp - offset_roll
    
    # Pitch complement value
    pitch_comp = (pitch_comp + data['gyr_y'] * dt) * 0.95 + (pitch_acc_temp) * 0.05
    
    # Roll complement value
    roll_comp = (roll_comp + data["gyr_x"] * dt) * 0.95 + (roll_acc_temp) *0.05

    # Getting final data
    data = {
        "pitch": pitch_comp,
        "roll": roll_comp,
        "Count": data['Count'],
        "DeviceID": data['DeviceID'],
        "Timer": data['timer']
    }

    # Returning the data value
    return data

# Function to connect to the topic in the AWS IoT
def on_connect(client, userdata, flags, rc):
    print("Connection returned result: " + str(rc))
    client.subscribe("stm32/sensor", qos = 1)

# Function to be executed when we get a message in the topic -> data collection
def on_message(client, userdata, msg):
    global list_in_message, count
    data = json.loads(msg.payload)
    
    # Calculating pitch and roll
    data = pitch_roll_calculation(data)

    # Updating the counter
    count = count + 1
    print (count)

    # Appending the data to a global list
    list_in_message.append(data)

# Function to perform real-time plot
def on_message_plot(client, userdata, msg):
    global list_in_message, count
    data = json.loads(msg.payload)
    
    # Calculating pitch and roll
    data = pitch_roll_calculation(data)

    # Updating the counter
    count = count + 1
    print (count)

    # Appending the data to a global list
    list_in_message.append(data)

    # Printing data
    print (data)

    # For real-time plotting, last 50 data (window size)
    plot_data = list_in_message[-window_size:]

    # Converting the data to pandas df
    df = pd.DataFrame.from_records(plot_data)

    # Plotting the data
    plt.plot(range(len(df)), df['pitch'], label='pitch')
    plt.plot(range(len(df)), df['roll'], label='roll')
    plt.legend()
    plt.show()

# MQTT connect credentials
mqttc = paho.Client()
mqttc.on_connect = on_connect

# For plotting or not plotting
if plot == "Y":
    mqttc.on_message = on_message_plot

else:    
    mqttc.on_message = on_message

awshost = "ap0risurn87nu-ats.iot.us-east-1.amazonaws.com"
awsport = 8883
caPath = "AmazonRootCA1.txt"
certPath = "5eb171f3d0-certificate.pem.crt"
keyPath= "5eb171f3d0-private.pem.key"
mqttc.tls_set(caPath, certfile=certPath, keyfile=keyPath, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)

# Connecting and calling the function in an infinite loop
mqttc.connect(awshost, awsport, keepalive=60)

# Do it till we get a keyboard interept
try:
    mqttc.loop_forever()

# If keyboard interrupt happens
except KeyboardInterrupt:
    # Converting it into pandas df
    df = pd.DataFrame.from_records(list_in_message)
    print (df.head())
    print ("Length of the data: " + str(len(df)))

    # Save local
    df.to_csv(output_filename + ".csv", index=False)

    # Uploading the file to S3
    #upload_to_s3(c, output_bucket, output_key, output_filename, filetype='csv')

    # Stopping the script
    sys.exit()