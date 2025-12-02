#! /usr/bin/python3
import usb.core
import usb.util 
import time
import logging
import argparse
import sys

#Description                | Command Code (Byte 0)
#Get Device Model Name      | 40
#Get Device Serial Number   | 41
#Send SCPI Command          | 1
#Get Firmware               | 99
#Set Attenuation            | 19
#Read Attenuation           | 18

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger("attenuator")

#find our device
dev = usb.core.find(idVendor=0x20ce, idProduct=0x0023)
# if dev is None:
#     raise ValueError('Device not found')
# logger.info(f"Device: {dev}")
# for cfg in dev:
#     for intf in cfg:
#         logger.info(f"Interface: {intf}")
#         for ep in intf:
#             logger.info(f"Endpoint: {ep}")
#         ifnum = intf.bInterfaceNumber
#         if not dev.is_kernel_driver_active(ifnum):
#             continue
#         try:
#             dev.detach_kernel_driver(ifnum)
#         except usb.core.USBError as e:
#             logger.warning(f"Detach kernel driver failed: {e}")
# #set the active configuration. with no args we use first config.
# logger.info("Setting active configuration")
# dev.set_configuration()
# SerialN = ""
# ModelN = ""
# Fw = ""

def query_device_info(dev):
    # Query Device Model Name (Command Code 40)
    cmd_model = bytearray(64)
    cmd_model[0] = 40
    logger.info("Querying Device Model Name (cmd 40)")
    dev.write(1, cmd_model)
    time.sleep(0.1)
    resp_model = dev.read(0x81, 64, timeout=1000)
    logger.debug(f"Raw Model Name response: {resp_model}")
    logger.debug(f"Model Name response (hex): {[hex(b) for b in resp_model]}")
    logger.info(f"Model Name response (ascii): {''.join([chr(b) if 32 <= b < 127 else '.' for b in resp_model])}")

    # Query Device Serial Number (Command Code 41)
    cmd_serial = bytearray(64)
    cmd_serial[0] = 41
    logger.info("Querying Device Serial Number (cmd 41)")
    dev.write(1, cmd_serial)
    time.sleep(0.1)
    resp_serial = dev.read(0x81, 64, timeout=1000)
    logger.debug(f"Raw Serial Number response: {resp_serial}")
    logger.debug(f"Serial Number response (hex): {[hex(b) for b in resp_serial]}")
    logger.info(f"Serial Number response (ascii): {''.join([chr(b) if 32 <= b < 127 else '.' for b in resp_serial])}")

    # Query Firmware Version (Command Code 99)
    cmd_fw = bytearray(64)
    cmd_fw[0] = 99
    logger.info("Querying Firmware Version (cmd 99)")
    dev.write(1, cmd_fw)
    time.sleep(0.1)
    resp_fw = dev.read(0x81, 64, timeout=1000)
    logger.debug(f"Raw Firmware response: {resp_fw}")
    logger.debug(f"Firmware response (hex): {[hex(b) for b in resp_fw]}")
    logger.info(f"Firmware response (ascii): {''.join([chr(b) if 32 <= b < 127 else '.' for b in resp_fw])}")

# # Set Attenuation (Command Code 19, example: set channel 1 to 11.25 dB)
# cmd_setatt = bytearray(64)
# cmd_setatt[0] = 0x13  # Command code 19
# cmd_setatt[1] = 0x01  # Channel 1
# cmd_setatt[2] = int(11.25 * 4)  # Attenuation in quarter dB steps (11.25 dB * 4 = 45)
# logger.info("Setting Attenuation (cmd 19, ch 1, 11.25 dB)")
# dev.write(1, cmd_setatt)
# time.sleep(0.1)
# resp_setatt = dev.read(0x81, 64, timeout=1000)
# logger.debug(f"Raw SetAtt response: {resp_setatt}")
# logger.debug(f"SetAtt response (hex): {[hex(b) for b in resp_setatt]}")
# logger.info(f"SetAtt response (ascii): {''.join([chr(b) if 32 <= b < 127 else '.' for b in resp_setatt])}")

# # Read Attenuation (Command Code 18, example: read channel 1)
# cmd_readatt = bytearray(64)
# cmd_readatt[0] = 0x12  # Command code 18
# cmd_readatt[1] = 0x01  # Channel 1
# logger.info("Reading Attenuation (cmd 18, ch 1)")
# dev.write(1, cmd_readatt)
# time.sleep(0.1)
# resp_readatt = dev.read(0x81, 64, timeout=1000)
# logger.debug(f"Raw ReadAtt response: {resp_readatt}")
# logger.debug(f"ReadAtt response (hex): {[hex(b) for b in resp_readatt]}")
# logger.info(f"ReadAtt response (ascii): {''.join([chr(b) if 32 <= b < 127 else '.' for b in resp_readatt])}")

def set_attenuation(dev, attenuation_db, channel_no):
    # Check for valid .25 dB increments
    if round((attenuation_db * 4) % 1, 8) != 0:
        logger.error(f"Attenuation {attenuation_db} dB is not a valid .25 dB increment.")
        raise ValueError("Attenuation must be in 0.25 dB increments.")
    
    cmd = bytearray(64)
    cmd[0] = 19  # Command code for Set Attenuation
    cmd[1] = int(attenuation_db)  # Att_Byte0
    cmd[2] = int(round((attenuation_db - int(attenuation_db)) * 4))  # Att_Byte1
    cmd[3] = channel_no  # Channel number
    logger.info(f"Setting Attenuation: {attenuation_db} dB on channel {channel_no}")
    logger.debug(f'command:{[hex(b) for b in cmd]}')
    dev.write(1, cmd)
    time.sleep(0.5)  # Allow time for command processing
    logger.info("Set command sent successfully")

def read_attenuation(dev):
    cmd = bytearray(64)
    cmd[0] = 18  # Command code for Read Attenuation
    logger.info("Reading Attenuation (cmd 18)")
    
    dev.write(1, cmd)
    time.sleep(0.5)  # Longer wait time
    
    try:
        resp = dev.read(0x81, 64, timeout=5000)  # Much longer timeout
        logger.debug(f"Raw ReadAtt response: {resp}")
        # Only print channel 1 attenuation for single channel attenuator
        att_byte0 = resp[1]
        att_byte1 = resp[2]
        attenuation = att_byte0 + (att_byte1 / 4.0)
        logger.info(f"Channel 1 Attenuation: {attenuation} dB (raw: {att_byte0}, {att_byte1})")
    except usb.core.USBTimeoutError as e:
        logger.error(f"Timeout reading from device: {e}")
        logger.info("Device may need to be reset. Try unplugging and reconnecting the USB device.")
        sys.exit(1)
    except usb.core.USBError as e:
        logger.error(f"USB communication error: {e}")
        logger.info("Device may need to be reset. Try unplugging and reconnecting the USB device.")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Programmable Attenuator Controller",
        usage="%(prog)s [-s VALUE | -r]"
    )
    parser.add_argument('-s', '--set', type=float, metavar='VALUE', help='Set attenuation to VALUE (dB) and return reading')
    parser.add_argument('-r', '--read', action='store_true', help='Read attenuation only')
    args = parser.parse_args()

    dev = usb.core.find(idVendor=0x20ce, idProduct=0x0023)
    if dev is None:
        print('Device not found')
        sys.exit(1)
    # Uncomment and use kernel driver detach/config if needed
    # Check if kernel driver is active and detach if necessary
    kernel_driver_detached = False
    logger.info("Checking kernel driver status")
    for cfg in dev:
        for intf in cfg:
            ifnum = intf.bInterfaceNumber
            if dev.is_kernel_driver_active(ifnum):
                try:
                    logger.info(f"Detaching kernel driver from interface {ifnum}")
                    dev.detach_kernel_driver(ifnum)
                    kernel_driver_detached = True
                    logger.info(f"Successfully detached kernel driver from interface {ifnum}")
                except usb.core.USBError as e:
                    logger.warning(f"Detach kernel driver failed: {e}")
    
    if not kernel_driver_detached:
        logger.info("No kernel driver was active")
    
    logger.info("Setting active configuration")
    dev.set_configuration()
    
    # Only reset device if we had to detach a kernel driver OR if we detect device issues
    if kernel_driver_detached:
        logger.info("Resetting USB device after kernel driver detachment")
        try:
            dev.reset()
            time.sleep(1.0)  # Wait for device to reset properly
            logger.info("USB device reset successful")
        except usb.core.USBError as e:
            logger.warning(f"USB reset failed: {e}")
    else:
        # Test if device is responsive, reset if needed
        try:
            logger.debug("Testing device responsiveness")
            test_cmd = bytearray(64)
            test_cmd[0] = 18  # Read command
            dev.write(1, test_cmd)
            time.sleep(0.1)
            dev.read(0x81, 64, timeout=500)
            logger.debug("Device responsive")
        except (usb.core.USBTimeoutError, usb.core.USBError):
            logger.info("Device not responsive, performing reset")
            try:
                dev.reset()
                time.sleep(1.0)
                logger.info("USB device reset successful")
            except usb.core.USBError as e:
                logger.warning(f"USB reset failed: {e}")
    
    # Claim the interface
    logger.info("Claiming interface")
    try:
        usb.util.claim_interface(dev, 0)
        logger.info("Successfully claimed interface 0")
    except usb.core.USBError as e:
        logger.warning(f"Failed to claim interface: {e}")
    
    # Brief stabilization delay
    time.sleep(0.1)

    if args.set is not None:
        set_attenuation(dev, args.set, 1)
        time.sleep(2)  # Allow device to settle after set command
        read_attenuation(dev)
    elif args.read:
        time.sleep(1)
        read_attenuation(dev)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()