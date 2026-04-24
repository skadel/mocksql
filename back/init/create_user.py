import asyncio
from datetime import datetime
from typing import Optional

import bcrypt

from common_vars import USERS_TABLE_NAME
from models.database import execute, query


async def create_user(
    email: str,
    password: str,
    name: str,
    picture: Optional[str] = None,
    role: str = "user",
) -> None:
    """
    Crée un utilisateur avec hachage du mot de passe.
    Utilise `query` pour vérifier l'existence et `execute` pour l'insertion.
    """
    # Hachage du mot de passe
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    now = datetime.utcnow().isoformat()

    # Vérifier si l'utilisateur existe (par email)
    existing = await query(
        f"SELECT 1 FROM {USERS_TABLE_NAME} WHERE email = $1", (email,)
    )
    if existing:
        raise ValueError("Un utilisateur avec cet email existe déjà.")

    sql = f"""
        INSERT INTO {USERS_TABLE_NAME}
            (user_id, email, password, name, picture, role, created_at)
        VALUES
            ($1,      $2,    $3,       $4,   $5,      $6,   $7)
        """
    params = (email, email, hashed, name, picture, role, now)

    await execute(sql, params)


# Main function to run the script
def main():
    email = input("Enter email: ").strip()
    password = input("Enter password: ")
    while True:
        name = input("Enter name: ").strip()
        if name:
            break
        print("Le champ 'name' est obligatoire. Veuillez entrer un nom valide.")

    picture = input("Enter picture URL (optional): ").strip() or None

    # Nouveau prompt pour le rôle
    choice = (
        input("Enter role – 'u' for user, 'a' for admin (default 'u'): ")
        .strip()
        .lower()
    )
    if choice == "a":
        role = "admin"
    else:
        role = "user"

    # Run the async function
    asyncio.run(create_user(email, password, name, picture, role))


if __name__ == "__main__":
    main()
