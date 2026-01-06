import time


def test_sleep(lbench):
    def sleep_function():
        time.sleep(0.1)

    lbench(sleep_function)
