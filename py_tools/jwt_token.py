import jwt


def encode_token(payload, secret=''):
    output = jwt.encode(payload, secret, algorithm='HS256')
    if isinstance(output, str):
        return output
    return output.decode()


def decode_token(payload, secret=''):
    return jwt.decode(payload, secret, algorithms=['HS256'])
