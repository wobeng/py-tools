import json

from jwcrypto import jwt, jwk


def encode_token(payload, secret=""):
    key = jwk.JWK(k=secret, kty="oct")
    token = jwt.JWT(header={"alg": "HS256", "typ": "JWT"}, claims=payload)
    token.make_signed_token(key)
    output = token.serialize()
    if isinstance(output, str):
        return output
    return output.decode()


def decode_token(payload, secret=""):
    key = jwk.JWK(k=secret, kty="oct")
    jwt_token = jwt.JWT(key=key, jwt=payload)
    return json.loads(jwt_token.claims)


def encrypt_token_asymmetric(payload, public_key_pem):
    jwk_key = jwk.JWK.from_pem(public_key_pem.encode("UTF-8"))
    header = {"enc": "A128CBC-HS256", "alg": "RSA-OAEP"}
    jwt_token = jwt.JWT(header, payload)
    jwt_token.make_encrypted_token(jwk_key)
    return jwt_token.serialize()


def decrypt_token_asymmetric(token, private_key_pem):
    jwk_key = jwk.JWK.from_pem(private_key_pem.encode("UTF-8"))
    jwt_token = jwt.JWT(
        key=jwk_key, jwt=token, algs=["A128CBC-HS256", "RSA-OAEP"]
    )
    return json.loads(jwt_token.claims)


def encrypt_token_symmetric(payload, secret):
    jwk_key = jwk.JWK.from_password(secret)
    header = {"alg": "PBES2-HS256+A128KW", "enc": "A256CBC-HS512"}
    jwt_token = jwt.JWT(header, payload)
    jwt_token.make_encrypted_token(jwk_key)
    return jwt_token.serialize()


def decrypt_token_symmetric(token, secret):
    jwk_key = jwk.JWK.from_password(secret)
    jwt_token = jwt.JWT(
        key=jwk_key, jwt=token, algs=["PBES2-HS256+A128KW", "A256CBC-HS512"]
    )
    return json.loads(jwt_token.claims)
