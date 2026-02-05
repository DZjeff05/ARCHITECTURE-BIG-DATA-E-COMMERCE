from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.security import create_access_token, get_current_user

router = APIRouter(prefix="/auth", tags=["auth"])

# ⚠️ Pour l'examen: mock users simple (à remplacer si besoin)
USERS = {
    "alice": {"password": "alice123", "role": "admin"},
    "reader": {"password": "reader123", "role": "reader"},
}

class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/login")
def login(body: LoginRequest):
    user = USERS.get(body.username)
    if not user or user["password"] != body.password:
        return {"error": "Invalid credentials"}  # simple; tu peux aussi lever 401

    token = create_access_token(username=body.username, role=user["role"])
    return {"access_token": token, "token_type": "bearer"}

@router.get("/me")
def me(user=Depends(get_current_user)):
    return {"username": user.get("sub"), "role": user.get("role")}
