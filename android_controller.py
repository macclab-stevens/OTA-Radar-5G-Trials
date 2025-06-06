import subprocess
from time import sleep

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
        pid = self.run_adb_command("shell pgrep iperf3")
        if pid:   
            print(f"Stopping iperf3 with PID: {pid}")
            # use CTRL+C
            self.run_adb_command("shell input tap 215 1540") #tap the CTRL button
            self.run_adb_command("shell input keyevent 31") #key input for "c"
            self.run_adb_command("shell input keyevent 66") #Press Enter key
            pid = self.run_adb_command("shell pgrep iperf3")
            if not pid:
                print( "iperf3 stopped successfully.")
                return True
            else:
                print(f"Failed to stop iperf3. PID still exists: {pid}")
                return False
        else:
            print("iperf3 is not running.")
            return True
    
    def restart_termux_iperf3(self):
        self.ensure_screen_on()
        # start iperf3 in termux
        if self.stop_termux_iperf3():
            print("Starting iperf3 in Termux...")
            self.run_adb_command("shell input text '/data/data/com.termux/files/home/iperf3.18%s-s' ") #%s is required space
            self.run_adb_command("shell input keyevent 66") #Press Enter key

# Example usage:
UE = AndroidController()
UE.restart_termux_iperf3()
# print(controller.enable_airplane_mode())
# print(controller.disable_airplane_mode())