import time

class Chrono:
    def __init__(self):
        self.start_time = None
        self.pause_time = None
        self.paused_duration = 0

    def start(self):
        self.start_time = time.time()

    def pause(self):
        if self.start_time is not None and self.pause_time is None:
            self.pause_time = time.time()

    def resume(self):
        if self.start_time is not None and self.pause_time is not None:
            self.paused_duration += time.time() - self.pause_time
            self.pause_time = None

    def stop(self):
        if self.start_time is not None:
            elapsed_time = time.time() - self.start_time - self.paused_duration
            self.start_time = None
            self.pause_time = None
            self.paused_duration = 0
            return elapsed_time
        return 0
