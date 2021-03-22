import json

import jwt
from jwcrypto import jwt, jwk


def encode_token(payload, secret=''):
    key = jwk.JWK(k=secret, kty='oct')
    token = jwt.JWT(header={'alg': 'HS256'}, claims=payload)
    token.make_signed_token(key)
    output = token.serialize()
    if isinstance(output, str):
        return output
    return output.decode()


def decode_token(payload, secret=''):
    key = jwk.JWK(k=secret, kty='oct')
    jwt_token = jwt.JWT(key=key, jwt=payload)
    return json.loads(jwt_token.claims)


def encrypt_token(payload, public_key_pem):
    jwk_key = jwk.JWK.from_pem(public_key_pem.encode('UTF-8'))
    header = {'enc': 'A128CBC-HS256', 'alg': 'RSA-OAEP'}
    jwt_token = jwt.JWT(header, payload)
    jwt_token.make_encrypted_token(jwk_key)
    return jwt_token.serialize()


def decrypt_token(token, private_key_pem):
    jwk_key = jwk.JWK.from_pem(private_key_pem.encode('UTF-8'))
    jwt_token = jwt.JWT(key=jwk_key, jwt=token, algs=['A128CBC-HS256', 'RSA-OAEP'])
    return json.loads(jwt_token.claims)
