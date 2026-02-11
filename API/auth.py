import os
from datetime import datetime, timedelta
from jose import jwt
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

JWT_SECRET = os.getenv("JWT_SECRET", "change_me")
JWT_ALGO = os.getenv("JWT_ALGO", "HS256")
JWT_EXPIRES_MIN = int(os.getenv("JWT_EXPIRES_MIN", "60"))

# Pour le devoir: user simple via variables d'environnement
API_USER = os.getenv("API_USER", "admin")
# On accepte soit un password clair (simple), soit un hash bcrypt
API_PASSWORD = os.getenv("API_PASSWORD", "admin123")
API_PASSWORD_HASH = os.getenv("API_PASSWORD_HASH", "")

def verify_password(plain: str) -> bool:
    if API_PASSWORD_HASH:
        return pwd_context.verify(plain, API_PASSWORD_HASH)
    return plain == API_PASSWORD

def authenticate(username: str, password: str) -> bool:
    return username == API_USER and verify_password(password)

def create_access_token(sub: str):
    exp = datetime.utcnow() + timedelta(minutes=JWT_EXPIRES_MIN)
    payload = {"sub": sub, "exp": exp}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)

def decode_token(token: str):
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
