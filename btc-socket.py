import socket
import struct
import mysql.connector
from datetime import datetime

# Define the structure sizes based on BTC_SNAP_DATA
NUMBER_TANKS = 8
BTC_DATETIME_FORMAT = "6B"  # Year, Month, Date, Hour, Min, Sec
BTC_TANKSTATE_FORMAT = "B B H f"  # solenoid, maxStatus, maxRTD, tankTemp
BTC_TANKCONFIG_FORMAT = "f f f B"  # tempTarget, pt100Cal, degreePerDay, controlMode
BTC_SNAP_DATA_FORMAT = (
    f"@H {BTC_DATETIME_FORMAT} B B B B"  # DeviceID, BTC_DATETIME, pumpStatus, logSnap
    f"{NUMBER_TANKS * BTC_TANKSTATE_FORMAT} "  # BTC_TANKSTATE array
    f"{NUMBER_TANKS * BTC_TANKCONFIG_FORMAT} "  # BTC_TANKCONFIG array
    f"{NUMBER_TANKS}I"  # solenoidTime array
)
BTC_SNAP_DATA_SIZE = struct.calcsize(BTC_SNAP_DATA_FORMAT)

# Database connection details
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'tank_management',
    'user': 'tanker',
    'password': '1234!@#$',
}

def datetime_to_string(date_time):
    return datetime(
        year=2000 + date_time[0],
        month=date_time[1],
        day=date_time[2],
        hour=date_time[3],
        minute=date_time[4],
        second=date_time[5]
    ).strftime('%Y-%m-%d %H:%M:%S')

def get_device_id(device_id):
    """Retrieve device_id from the devices table using board_id."""
    board_id = f"BTC-{device_id}"
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = "SELECT id FROM devices WHERE board_id = %s"
        cursor.execute(query, (board_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
def insert_device_if_new(device_id):
    board_id = f"BTC-{device_id}"
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Check if device already exists
        check_query = "SELECT id FROM devices WHERE board_id = %s"
        cursor.execute(check_query, (board_id,))
        device = cursor.fetchone()

        if not device:
            # Insert new device
            insert_query = "INSERT INTO devices (board_id, name) VALUES (%s, %s)"
            cursor.execute(insert_query, (board_id, f"Device {device_id}"))
            conn.commit()
            print(f"Inserted new device with board_id: {board_id}")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def send_config_updates(conn, device_id):
    try:
        conn_db = mysql.connector.connect(**DB_CONFIG)
        cursor = conn_db.cursor()

        # Fetch configurations where update_flag = 1
        fetch_query = """
            SELECT tank_id, target_temp, pt100_cal, degree_per_day, control_mode 
            FROM tank_configs 
            WHERE device_id = %s AND update_flag = 1
        """
        cursor.execute(fetch_query, (device_id,))
        configs = cursor.fetchall()

        if not configs:
            print(f"No configurations to update for Device ID {device_id}")
            return

        # Prepare packed_data with command and count fields
        COMMAND_UPDATE_CONFIGS = 0x15  # Define the command byte for updating configs
        packed_data = struct.pack("BB", COMMAND_UPDATE_CONFIGS, len(configs))
        
        for config in configs:
            tank_id, target_temp, pt100_cal, degree_per_day, control_mode = config
            packed_data += struct.pack("B", tank_id)
            packed_data += struct.pack(BTC_TANKCONFIG_FORMAT, target_temp, pt100_cal, degree_per_day, control_mode)

            # Reset the update_flag to 0
            update_query = "UPDATE tank_configs SET update_flag = 0 WHERE device_id = %s AND tank_id = %s"
            cursor.execute(update_query, (device_id, tank_id))

        # Send the packed data to the device
        conn.sendall(packed_data)
        print(f"Sent configuration updates to Device ID {device_id}")

        conn_db.commit()

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    except Exception as e:
        print(f"Error while sending config updates: {e}")
    finally:
        if conn_db.is_connected():
            cursor.close()
            conn_db.close()

def insert_log_and_update_status(device_id, date_time, tank_states, tank_configs, solenoid_times, log_snap):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Insert log into logs table (if logSnap is true)
        log_query = (
            "INSERT INTO logs (device_id, tank_id, time, current_temp, solenoid, max_rtd, max_status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        )

        # Insert if no match exists in tank_status
        insert_status_query = (
            "INSERT INTO tank_statuses (device_id, tank_id, sol_time, current_temp, solenoid, max_rtd, max_status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE sol_time=VALUES(sol_time), "
            "current_temp=VALUES(current_temp), solenoid=VALUES(solenoid), "
            "max_rtd=VALUES(max_rtd), max_status=VALUES(max_status)"
        )

        # Insert or update tank_configs table
        upsert_config_query = (
            "INSERT INTO tank_configs (device_id, tank_id, control_mode, target_temp, pt100_cal, degree_per_day) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE control_mode=VALUES(control_mode), "
            "target_temp=VALUES(target_temp), pt100_cal=VALUES(pt100_cal), "
            "degree_per_day=VALUES(degree_per_day)"
        )

        timestamp = datetime_to_string(date_time)

        for tank_id, (state, config, sol_time) in enumerate(zip(tank_states, tank_configs, solenoid_times), start=1):
            solenoid, max_status, max_rtd, tank_temp = state
            temp_target, pt100_cal, degree_per_day, control_mode = config

            if (tank_temp < 0):
                tank_temp = 0

            if solenoid !=0:
                solenoid = 1

            # Insert log entry if logSnap is true
            if log_snap:
                cursor.execute(log_query, (
                    device_id, tank_id, timestamp, tank_temp, solenoid, max_rtd, max_status
                ))
            
            # Insert into tank_status if no match exists
            cursor.execute(insert_status_query, (
                device_id, tank_id, sol_time, tank_temp, solenoid, max_rtd, max_status
            ))

            # Check if the tank's configuration update_flag is set to 1
            cursor.execute("SELECT update_flag FROM tank_configs WHERE device_id = %s AND tank_id = %s", (device_id, tank_id))
            update_flag = cursor.fetchone()

            # If update_flag is 1, don't update the configuration, skip it
            if update_flag and update_flag[0] == 1:
                print(f"Configuration for Tank {tank_id} is being updated, skipping update in the database.")
            else:
                # Upsert tank configuration
                cursor.execute(upsert_config_query, (
                    device_id, tank_id, control_mode, temp_target, pt100_cal, degree_per_day
                ))

        conn.commit()
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Start the TCP socket server
def start_server():
    host = '0.0.0.0'
    port = 9090

    # Create socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"Server listening on {host}:{port}")

        while True:
            conn, addr = server_socket.accept()
            print(f"Connection established with {addr}")
            handle_client(conn)

def handle_client(conn):
    with conn:
        while True:
            data = conn.recv(BTC_SNAP_DATA_SIZE)
            if not data:
                print("Client disconnected")
                break

            if len(data) == BTC_SNAP_DATA_SIZE:
                unpacked_data = struct.unpack(BTC_SNAP_DATA_FORMAT, data)

                board_id = unpacked_data[0]
                date_time = unpacked_data[1:7]
                pump_status = unpacked_data[7]
                log_snap = unpacked_data[8]

                tank_states = [
                    unpacked_data[11 + i * 4:15 + i * 4]
                    for i in range(NUMBER_TANKS)
                ]

                tank_configs = [
                    unpacked_data[11 + NUMBER_TANKS * 4 + i * 4:15 + NUMBER_TANKS * 4 + i * 4]
                    for i in range(NUMBER_TANKS)
                ]

                solenoid_times = unpacked_data[11 + NUMBER_TANKS * 8:11 + NUMBER_TANKS * 12]

                print("Device ID:", board_id)
                print("Date Time:", datetime_to_string(date_time))
                print("Pump Status:", pump_status)
                print("Log Sanp:", log_snap)

                for tank_id, (state, config, sol_time) in enumerate(zip(tank_states, tank_configs, solenoid_times), start=1):
                    solenoid, max_status, max_rtd, tank_temp = state
                    temp_target, pt100_cal, degree_per_day, control_mode = config

                    print(f"Tank {tank_id}: Solenoid={solenoid}, MaxStatus={max_status}, MaxRTD={max_rtd}, TankTemp={tank_temp}, SolTime={sol_time}")
                    print(f"Tank {tank_id} Config: TempTarget={temp_target}, PT100Cal={pt100_cal}, DegreePerDay={degree_per_day}, ControlMode={control_mode}")

                device_id = get_device_id(board_id)

                if not device_id:
                    print(f"No matching device found for board_id {board_id}")
                    insert_device_if_new(board_id)
                    
                # Insert and update database
                insert_log_and_update_status(device_id, date_time, tank_states, tank_configs, solenoid_times, log_snap)
                send_config_updates(conn, device_id)
            else:
                print(f"Invalid data size: expected {BTC_SNAP_DATA_SIZE}, got {len(data)}")

if __name__ == "__main__":
    start_server()
