from datetime import timedelta, datetime
from typing import Annotated

import jwt
from fastapi import APIRouter, Depends, Header, Form
from pydantic import BaseModel
from sqlalchemy import text
from fastapi import FastAPI, HTTPException, Header
from passlib.hash import pbkdf2_sha512 as pl

import secret
from db import DW

router = APIRouter(
    prefix='/api/v1/auth',
    tags=['Auth']
)

SECRET_KEY = secret.SECRET_KEY

def require_login(dw: DW, authorization=Header(None, alias="api_key")):
    try:
        if authorization is not None and len(authorization.split(" ")) == 2:
            split_header = authorization.split(" ")
            if len(split_header) == 2 and split_header[0] == "Bearer":
                token = split_header[1]
                validated = jwt.decode(token, SECRET_KEY, algorithms=["HS512"])
                user = dw.execute(text("SELECT username FROM users WHERE id = :id"),
                                  {"id": validated["id"]}).mappings().first()
                if user is None:
                    raise HTTPException(detail="user not found", status_code=404)
                return user
        else:
            raise HTTPException(detail="unauthorized", status_code=401)
    except Exception as e:
        raise HTTPException(detail=str(e), status_code=500)

LoggedInUser = Annotated[dict, Depends(require_login)]

class RegisterRequest(BaseModel):
    username: str
    password: str

@router.get("/account")
async def get_account(logged_in_user: LoggedInUser):
    return logged_in_user

@router.post("/register")
async def register(dw: DW, req: RegisterRequest):
    try:
        _query_str = ("INSERT INTO users (username, password) VALUES(:username, :password)")
        _query = text(_query_str)
        user = dw.execute(_query, {"username": req.username, "password": pl.hash(req.password)})
        dw.commit()
        return {"username": req.username, "id": user.lastrowid}
    except Exception as e:
        dw.rollback()
        print(e)
        raise HTTPException(status_code=422, detail="error registering user")

@router.post("/login")
async def login(dw: DW, req: RegisterRequest):
    _query_str = ("SELECT * FROM users WHERE username = :username")
    _query = text(_query_str)
    user = dw.execute(_query, {"username": req.username}).mappings().first()
    if user is None:
        raise HTTPException(detail="user not found", status_code=404)

    password_correct = pl.verify(req.password, user["password"])
    if password_correct:
        token = jwt.encode({"id": user["id"]}, SECRET_KEY, algorithm="HS512")
        return {"token": token}
    raise HTTPException(detail="user not found", status_code=404)

@router.post("/logout")
async def logout(dw: DW):
    try:
        return {"message": "Logged out successfully"}
    except Exception as e:
        raise HTTPException(detail=f"{e}", status_code=500)


@router.put("/update/{username}")
async def update_user(username: str, new_username: str, new_password: str, dw: DW):
    _query_str = "UPDATE users SET username = :new_username, password = :new_password WHERE username = :username"
    _query = text(_query_str)
    result = dw.execute(_query, {"username": username, "new_username": new_username, "new_password": new_password})
    dw.commit()
    if result.rowcount == 0:
        raise HTTPException(detail="User not found", status_code=404)

    return {"message": "User updated successfully"}

@router.delete("/delete")
async def delete_user(dw: DW, username: str):
    _query_str = "DELETE FROM users WHERE username = :username"
    _query = text(_query_str)
    result = dw.execute(_query, {"username": username})
    dw.commit()

    if result.rowcount == 0:
        raise HTTPException(detail="User not found", status_code=404)

    return {"message": "User deleted successfully"}


