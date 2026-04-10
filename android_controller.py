#! /usr/bin/python3
import subprocess
from time import sleep
import argparse
import sys

class AndroidController:
    def __init__(self, adb_path='adb'):
        self.adb_path = adb_path

    def run_adb_command(self, command):
        full_command = f"{self.adb_path} {command}"
        try:
            result = subprocess.run(full_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return result.stdout.decode('utf-8').strip()
        except subprocess.CalledProcessError as e:
            return e.stderr.decode('utf-8').strip()

    def enable_airplane_mode(self):
        print("Enabling airplane mode...")
        return self.run_adb_command('shell cmd connectivity airplane-mode enable')

    def disable_airplane_mode(self):
        print("Disabling airplane mode...")
        return self.run_adb_command('shell cmd connectivity airplane-mode disable')
    
    def get_lockScreen_status(self):
        output =  self.run_adb_command('shell dumpsys nfc')
        # print(output)
        mScreenState = None
        for line in output.splitlines():
            if 'mScreenState' in line:
                 mScreenState = line.split("=")[1].strip()
        return mScreenState
    def ensure_screen_on(self):
        # Ensure the screen is on
        screenState = self.get_lockScreen_status()
        print(f"screenState: {screenState}")
        if screenState == 'OFF_LOCKED':
            # Unlock the screen
            self.run_adb_command('shell input keyevent 82')
            sleep(0.5)
            # Swipe up to unlock
            self.run_adb_command('shell input swipe 200 500 200 000')
        elif screenState == 'ON_LOCKED':
            # Just turn the screen on
            self.run_adb_command('shell input swipe 200 500 200 000')
        elif screenState == 'OFF_UNLOCKED':
            return True
        screenState = self.get_lockScreen_status()
        if self.get_lockScreen_status() == 'OFF_UNLOCKED':
            return True
        else:
            return False
    def stop_termux_iperf3(self):
        self.ensure_screen_on()
        print("Stopping iperf3...")
        # use pkill command
        self.run_adb_command("shell input text 'pkill%siperf3'") #%s is required space
        self.run_adb_command("shell input keyevent 66") #Press Enter key
        sleep(0.5)  # Give it a moment to process
        pid = self.run_adb_command("shell pgrep iperf3")
        if not pid:
            print("iperf3 stopped successfully.")
            return True
        else:
            print(f"Failed to stop iperf3. PID still exists: {pid}")
            return False
    
    def restart_termux_iperf3(self):
        self.ensure_screen_on()
        # start iperf3 in termux
        if self.stop_termux_iperf3():
            print("Starting iperf3 in Termux...")
            self.run_adb_command("shell input text '/data/data/com.termux/files/home/iperf3.18%s-s%s-p%s5201%s-D' ") #%s is required space
            self.run_adb_command("shell input keyevent 66") #Press Enter key
            self.run_adb_command("shell input text '/data/data/com.termux/files/home/iperf3.18%s-s%s-p%s5202%s-D' ") #%s is required space
            self.run_adb_command("shell input keyevent 66") #Press Enter key

def main():
    parser = argparse.ArgumentParser(
        description="Android ADB Controller",
        usage="%(prog)s [-o | -O | -s | -r] [--adb-path PATH]"
    )
    parser.add_argument('-o', '--enable-airplane', action='store_true', help='Enable airplane mode')
    parser.add_argument('-O', '--disable-airplane', action='store_true', help='Disable airplane mode')
    parser.add_argument('-s', '--stop-iperf3', action='store_true', help='Stop iperf3 in Termux')
    parser.add_argument('-r', '--restart-iperf3', action='store_true', help='Restart iperf3 in Termux')
    parser.add_argument('--adb-path', type=str, default='adb', help='Path to adb executable')
    args = parser.parse_args()

    UE = AndroidController(adb_path=args.adb_path)

    if args.enable_airplane:
        print(UE.enable_airplane_mode())
    elif args.disable_airplane:
        print(UE.disable_airplane_mode())
    elif args.stop_iperf3:
        result = UE.stop_termux_iperf3()
        sys.exit(0 if result else 1)
    elif args.restart_iperf3:
        UE.restart_termux_iperf3()
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()