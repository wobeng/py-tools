import random
import bcrypt
import time


def pin_generator():
    return random.randint(100000, 999999)


def gen_otp(pin=None, expire_in_seconds=600):
    pin = str(pin or pin_generator())
    pin_hashed = bcrypt.hashpw(pin.encode("utf-8"), bcrypt.gensalt()).decode()
    return {
        "pin": pin,
        "pin_hashed": pin_hashed,
        "expire": int(time.time()) + expire_in_seconds,
    }


def otp_valid(pin, pin_hashed, expire_in_seconds):
    if expire_in_seconds < int(time.time()):
        return False
    if not bcrypt.checkpw(
        password=str(pin).encode(), hashed_password=pin_hashed.encode()
    ):
        return False
    return True
