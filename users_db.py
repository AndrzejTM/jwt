import bcrypt
from typing import Dict, Any, List, Union

# Hasło "admin123" po zahashowaniu
# W tym przykładzie, generujemy hasło dla "admin"
hashed_pw_admin = bcrypt.hashpw(b"admin123", bcrypt.gensalt())
# Hasło dla zwykłego użytkownika "user123"
hashed_pw_user = bcrypt.hashpw(b"user123", bcrypt.gensalt())

# Reprezentacja modelu User z polem 'roles' dla zadania 6
class User:
    def __init__(self, id: int, username: str, hashed_password: bytes, roles: List[str]):
        self.id = id
        self.username = username
        self.hashed_password = hashed_password
        self.roles = roles

# Użycie tymczasowej bazy danych (słownika)
USERS_DB: Dict[str, User] = {
    "admin": User(
        id=1,
        username="admin",
        hashed_password=hashed_pw_admin,
        roles=["ROLE_ADMIN", "ROLE_USER"]
    ),
    "jan.kowalski": User(
        id=2,
        username="jan.kowalski",
        hashed_password=hashed_pw_user,
        roles=["ROLE_USER"]
    )
}

# Poprawiona adnotacja typu dla kompatybilności z Pythonem < 3.10
def get_user_by_username(username: str) -> Union[User, None]:
    """Pobiera użytkownika z tymczasowej bazy danych."""
    return USERS_DB.get(username)

def add_user_to_db(user: User):
    """Dodaje nowego użytkownika (tymczasowo) do bazy."""
    if user.username not in USERS_DB:
        USERS_DB[user.username] = user
        return True
    return False