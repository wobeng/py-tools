import jwt


def encode_token(payload, secret=''):
    return jwt.encode(payload, secret).decode()


def decode_token(payload, secret=''):
    return jwt.decode(payload, secret)
