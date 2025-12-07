from fastapi import FastAPI, HTTPException, Depends, Header, status
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone
import jwt
import bcrypt
# Poprawione importy dla kompatybilności z Pythonem 3.8
from typing import Optional, List, Union

# Import z pliku users_db.py
from users_db import get_user_by_username, add_user_to_db, User, USERS_DB

# --- Konfiguracja JWT ---
SECRET_KEY = "super_secret_key_bardzo_tajny"
ALGORITHM = "HS256"


# --- Modele Pydantic ---

class LoginData(BaseModel):
    username: str
    password: str


class UserCreate(BaseModel):
    username: str
    password: str
    roles: List[str] = ["ROLE_USER"]


class UserDetails(BaseModel):
    id: int
    username: str
    roles: List[str]


# --- Inicjalizacja Aplikacji ---
app = FastAPI()


# --- Funkcje Pomocnicze JWT ---

def create_access_token(user: User) -> str:
    """Generuje token JWT dla podanego użytkownika."""
    payload = {
        "sub": str(user.id),
        "username": user.username,
        "roles": user.roles,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1)
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return token


def decode_access_token(token: str) -> dict:
    """Dekoduje i weryfikuje token JWT."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.InvalidSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token signature")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


# Użycie 'Optional' (dostępne od Python 3.5), które jest równoważne 'Union[T, None]'
def get_current_user(authorization: Optional[str] = Header(None)) -> UserDetails:
    """Zależność do weryfikacji tokena w nagłówku Authorization: Bearer <token>."""
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated. Missing Bearer token.")

    token = authorization.split(" ")[1]
    payload = decode_access_token(token)

    user_details = UserDetails(
        id=int(payload.get("sub")),
        username=payload.get("username"),
        roles=payload.get("roles", [])
    )
    return user_details


# --- Endpoints ---

@app.post("/login", response_model=dict)
def login(data: LoginData):
    """
    Endpoint /login przyjmujący login i hasło, a zwracający token JWT.
    """
    username = data.username
    password = data.password.encode('utf-8')

    user = get_user_by_username(username)
    if user is None:
        # W praktyce zawsze zwracamy ten sam błąd, aby nie ujawniać, czy login istnieje
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    hashed_pw = user.hashed_password

    # Porównanie hasła za pomocą bcrypt.checkpw
    if not bcrypt.checkpw(password, hashed_pw):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

        # Generowanie tokena
    token = create_access_token(user)

    return {"access_token": token, "token_type": "bearer"}


# --- Endpoiny Zabezpieczone (Zadania 4, 6, 7) ---

@app.get("/user_details", response_model=UserDetails)
def get_user_details(current_user: UserDetails = Depends(get_current_user)):
    """
    Endpoint zwracający dane zalogowanego użytkownika z payloadu tokena.
    """
    return current_user


# Funkcja pomocnicza do sprawdzania roli
def admin_required(current_user: UserDetails = Depends(get_current_user)):
    """Weryfikuje, czy użytkownik ma rolę ROLE_ADMIN."""
    if "ROLE_ADMIN" not in current_user.roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied. Admin role required.")
    return current_user


@app.post("/users", response_model=UserDetails, status_code=status.HTTP_201_CREATED)
def add_new_user(new_user_data: UserCreate, current_admin: UserDetails = Depends(admin_required)):
    """
    Dodaje nowego użytkownika do bazy danych. Dostępny tylko dla Adminów.
    """
    hashed_password = bcrypt.hashpw(new_user_data.password.encode('utf-8'), bcrypt.gensalt())

    new_id = len(USERS_DB) + 1
    new_user = User(
        id=new_id,
        username=new_user_data.username,
        hashed_password=hashed_password,
        roles=new_user_data.roles
    )

    if add_user_to_db(new_user):
        return UserDetails(id=new_user.id, username=new_user.username, roles=new_user.roles)
    else:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already exists")

# --- Uruchomienie Serwera (dla testów lokalnych) ---
# Aby uruchomić: uvicorn main:app --reload