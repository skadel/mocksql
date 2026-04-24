import datetime


COMMON_HISTORY_TABLE_NAME = "common_history"
SESSIONS_TABLE_NAME = "sessions"
LITERALS_TABLE_NAME = "lit"
EXAMPLES_TABLE_NAME = "examples"
USERS_TABLE_NAME = "users"
MODELS_TABLE_NAME = "models"
PROJECTS_TABLE_NAME = "projects"
USER_PROJECTS_TABLE_NAME = "user_projects"
PROJECT_TABLES_TABLE_NAME = "project_tables"
USER_SETTINGS_TABLE_NAME = "user_settings"

# TODO HERE WE CAN ADD is_nullable, regex, mutable
# is_nullable, regex, mutable and is_categorical can be used to automatically check on sources and generated tests
# tables_and_columns = [
#     {
#         "table_name": "bigquery-public-data.thelook_ecommerce.distribution_centers",
#         "columns": [
#             {"name": "id", "type": "INTEGER",
#              "description": "Unique identifier for each distribution center.",
#              "is_categorical": False},
#             {"name": "name", "type": "STRING", "description": "Name of the distribution center.",
#              "is_categorical": True},
#             {"name": "latitude", "type": "FLOAT",
#              "description": "Latitude coordinate of the distribution center.",
#              "is_categorical": False},
#             {"name": "longitude", "type": "FLOAT",
#              "description": "Longitude coordinate of the distribution center.",
#              "is_categorical": False}
#         ],
#         "primary_keys": ["id"]
#     },
#     {
#         "table_name": "bigquery-public-data.thelook_ecommerce.events",
#         "columns": [
#             {"name": "id", "type": "INTEGER", "description": "Unique identifier for each event.",
#              "is_categorical": False},
#             {"name": "user_id", "type": "INTEGER",
#              "description": "Identifier of the user associated with the event.",
#              "is_categorical": False},
#             {"name": "sequence_number", "type": "INTEGER",
#              "description": "Sequence number of the event in a session.",
#              "is_categorical": False},
#             {"name": "session_id", "type": "STRING",
#              "description": "Identifier of the session during which the event occurred.",
#              "is_categorical": False},
#             {"name": "created_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the event was created.",
#              "is_categorical": False},
#             {"name": "ip_address", "type": "STRING",
#              "description": "IP address of the user when the event occurred.",
#              "is_categorical": False},
#             {"name": "city", "type": "STRING", "description": "City from which the event was logged.",
#              "is_categorical": True,
#              "examples": ['Sapporo', 'São Paulo', 'Tieling']
#              },
#             {"name": "state", "type": "STRING", "description": "State from which the event was logged.",
#              "is_categorical": True,
#              "examples": ['New York', 'São Paulo', 'Hokkaido']},
#             {"name": "postal_code", "type": "STRING",
#              "description": "Postal code from which the event was logged.",
#              "is_categorical": False},
#             {"name": "browser", "type": "STRING", "description": "Browser used to access the service.",
#              "is_categorical": True,
#              "examples": ['IE', 'Chrome', 'Firefox', 'Other', 'Safari']},
#             {"name": "traffic_source", "type": "STRING",
#              "description": "Source of traffic that led to the event.",
#              "is_categorical": True,
#              "examples": ['Email', 'Facebook', 'YouTube', 'Adwords', 'Organic']},
#             {"name": "uri", "type": "STRING",
#              "description": "Uniform Resource Identifier accessed during the event.",
#              "is_categorical": True,
#              "examples": ['/cancel', '/cart', '/home', '/purchase',
#                           '/department/women/category/skirts/brand/allenallen', '/product/9331']},
#             {"name": "event_type", "type": "STRING", "description": "Type of event recorded.",
#              "is_categorical": True,
#              "examples": ['cancel', 'cart', 'home', 'department', 'product', 'purchase']}
#         ],
#         "primary_keys": ["id"]
#     },
#     {
#         "table_name": "bigquery-public-data.thelook_ecommerce.inventory_items",
#         "columns": [
#             {"name": "id", "type": "INTEGER", "description": "Unique identifier for each inventory item.",
#              "is_categorical": False},
#             {"name": "product_id", "type": "INTEGER",
#              "description": "Identifier of the product associated with the inventory item.",
#              "is_categorical": False},
#             {"name": "created_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the inventory item was created.",
#              "is_categorical": False},
#             {"name": "sold_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the inventory item was sold.",
#              "is_categorical": False},
#             {"name": "cost", "type": "FLOAT", "description": "Cost of the inventory item.",
#              "is_categorical": False},
#             {"name": "product_category", "type": "STRING", "description": "Category of the product.",
#              "is_categorical": True,
#              "examples": ['Accessories', 'Active', 'Blazers & Jackets', 'Clothing Sets', 'Suits',
#                           'Suits & Sport Coats']},
#             {"name": "product_name", "type": "STRING", "description": "Name of the product.",
#              "is_categorical": False},
#             {"name": "product_brand", "type": "STRING", "description": "Brand of the product.",
#              "is_categorical": True},
#             {"name": "product_retail_price", "type": "FLOAT",
#              "description": "Retail price of the product.",
#              "is_categorical": False},
#             {"name": "product_department", "type": "STRING",
#              "description": "Department (Women/Men) associated with the product.",
#              "is_categorical": True,
#              "examples": ['Women', 'Men']},
#             {"name": "product_sku", "type": "STRING", "description": "Stock Keeping Unit of the product.",
#              "is_categorical": False},
#             {"name": "product_distribution_center_id", "type": "INTEGER",
#              "description": "Identifier of the distribution center where the product is stored.",
#              "is_categorical": False}
#         ],
#         "primary_keys": ["id"]
#     },
#     {
#         "table_name": "bigquery-public-data.thelook_ecommerce.order_items",
#         "columns": [
#             {"name": "id", "type": "INTEGER", "description": "Unique identifier for each order item.",
#              "is_categorical": False},
#             {"name": "order_id", "type": "INTEGER",
#              "description": "Identifier of the order associated with the order item.",
#              "is_categorical": False},
#             {"name": "user_id", "type": "INTEGER",
#              "description": "Identifier of the user who placed the order.",
#              "is_categorical": False},
#             {"name": "product_id", "type": "INTEGER", "description": "Identifier of the product ordered.",
#              "is_categorical": False},
#             {"name": "inventory_item_id", "type": "INTEGER",
#              "description": "Identifier of the inventory item sold.",
#              "is_categorical": False},
#             {"name": "status", "type": "STRING",
#              "description": "Current status of the order item (e.g., shipped, delivered).",
#              "is_categorical": True},
#             {"name": "created_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the order item was created.",
#              "is_categorical": False},
#             {"name": "shipped_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the order item was shipped.",
#              "is_categorical": False},
#             {"name": "delivered_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the order item was delivered.",
#              "is_categorical": False},
#             {"name": "returned_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the order item was returned, if applicable.",
#              "is_categorical": False},
#             {"name": "sale_price", "type": "FLOAT", "description": "Sale price of the order item.",
#              "is_categorical": False}
#         ],
#         "primary_keys": ["id"]
#     },
#     {
#         "table_name": "bigquery-public-data.thelook_ecommerce.orders",
#         "columns": [
#             {"name": "order_id", "type": "INTEGER", "description": "Unique identifier for each order.",
#              "is_categorical": False},
#             {"name": "user_id", "type": "INTEGER",
#              "description": "Identifier of the user who placed the order.",
#              "is_categorical": False},
#             {"name": "status", "type": "STRING",
#              "description": "Current status of the order.",
#              "is_categorical": True,
#              "examples": ['Cancelled', 'Complete', 'Processing', 'Returned', 'Shipped']},
#             {"name": "gender", "type": "STRING", "description": "Gender of the user who placed the order.",
#              "is_categorical": True,
#              "examples": ['F', 'M']},
#             {"name": "created_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the order was created.",
#              "is_categorical": False},
#             {"name": "returned_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the order was returned, if applicable.",
#              "is_categorical": False},
#             {"name": "shipped_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the order was shipped.",
#              "is_categorical": False},
#             {"name": "delivered_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the order was delivered.",
#              "is_categorical": False},
#             {"name": "num_of_item", "type": "INTEGER",
#              "description": "Number of items included in the order.",
#              "is_categorical": False}
#         ],
#         "primary_keys": ["order_id"]
#     },
#     {
#         "table_name": "bigquery-public-data.thelook_ecommerce.products",
#         "columns": [
#             {"name": "id", "type": "INTEGER", "description": "Unique identifier for each product.",
#              "is_categorical": False},
#             {"name": "cost", "type": "FLOAT", "description": "Cost of the product.",
#              "is_categorical": False},
#             {"name": "category", "type": "STRING", "description": "Category of the product.",
#              "is_categorical": True,
#              "examples": ['Accessories', 'Plus', 'Swim', 'Active', 'Socks & Hosiery']},
#             {"name": "name", "type": "STRING", "description": "Name of the product.",
#              "is_categorical": True},
#             {"name": "brand", "type": "STRING", "description": "Brand of the product.",
#              "is_categorical": True,
#              "examples": ['Accessories', 'Plus', 'Swim', 'Active', 'Socks & Hosiery']},
#             {"name": "retail_price", "type": "FLOAT", "description": "Retail price of the product.",
#              "is_categorical": False},
#             {"name": "department", "type": "STRING",
#              "description": "Department associated with the product.",
#              "is_categorical": False,
#              "examples": ['Women', 'Men']},
#             {"name": "sku", "type": "STRING", "description": "Stock Keeping Unit of the product.",
#              "is_categorical": False},
#             {"name": "distribution_center_id", "type": "INTEGER",
#              "description": "Identifier of the distribution center that stocks the product.",
#              "is_categorical": False}
#         ],
#         "primary_keys": ["id"]
#     },
#     {
#         "table_name": "bigquery-public-data.thelook_ecommerce.users",
#         "columns": [
#             {"name": "id", "type": "INTEGER", "description": "Unique identifier for each user.",
#              "is_categorical": False},
#             {"name": "first_name", "type": "STRING", "description": "First name of the user.",
#              "is_categorical": False},
#             {"name": "last_name", "type": "STRING", "description": "Last name of the user.",
#              "is_categorical": False},
#             {"name": "email", "type": "STRING", "description": "Email address of the user.",
#              "is_categorical": False},
#             {"name": "age", "type": "INTEGER", "description": "Age of the user.",
#              "is_categorical": False},
#             {"name": "gender", "type": "STRING", "description": "Gender of the user.",
#              "is_categorical": True,
#              "examples": ['M', 'F']},
#             {"name": "state", "type": "STRING", "description": "State of residence of the user.",
#              "is_categorical": True,
#              "examples": ['Acre', 'Aichi', 'Akita']},
#             {"name": "street_address", "type": "STRING", "description": "Street address of the user.",
#              "is_categorical": False},
#             {"name": "postal_code", "type": "STRING", "description": "Postal code of the user's address.",
#              "is_categorical": False},
#             {"name": "city", "type": "STRING", "description": "City of residence of the user.",
#              "is_categorical": True,
#              "examples": ['Rio Branco', 'Ichinomiya City', 'Kiyosu City']},
#             {"name": "country", "type": "STRING", "description": "Country of residence of the user.",
#              "is_categorical": True,
#              "examples": ['Brasil', 'United States', 'Colombia']},
#             {"name": "latitude", "type": "FLOAT",
#              "description": "Latitude coordinate of the user's location.",
#              "is_categorical": False},
#             {"name": "longitude", "type": "FLOAT",
#              "description": "Longitude coordinate of the user's location.",
#              "is_categorical": False},
#             {"name": "traffic_source", "type": "STRING",
#              "description": "Source from which the user originated.",
#              "is_categorical": True,
#              "examples": ['Search', 'Organic', 'Email', 'Facebook', 'Display']},
#             {"name": "created_at", "type": "TIMESTAMP",
#              "description": "Timestamp when the user's record was created.",
#              "is_categorical": False}
#         ],
#         "primary_keys": ["id"]
#     }
# ]

type_mapping = {
    "INTEGER": int,
    "INT64": int,
    "INT": int,
    "STRING": str,
    "FLOAT": float,
    "FLOAT64": float,
    "DATE": datetime.date,
    "TIMESTAMP": datetime.datetime,
}


async def get_table_details(project_id: str, **_):
    from models.schemas import get_schemas

    content = []

    def _esc(s: str) -> str:
        return s.replace("{", "{{").replace("}", "}}")

    schema = await get_schemas(project_id)

    for table in schema:
        lines = [f"Table: {table['table_name']}"]

        # Description de la table
        table_desc = (table.get("description") or "").strip()
        if table_desc:
            lines.append(f"Description: {_esc(table_desc)}")

        # Primary keys (optionnel)
        pks = table.get("primary_keys") or []
        if pks:
            lines.append(f"Primary keys: {', '.join(pks)}")

        for col in table.get("columns", []):
            parts = []

            col_type = (col.get("type") or "").strip()
            if col_type:
                parts.append(f"Type: {_esc(col_type)}")

            description = (col.get("description") or "").strip()
            if description:
                parts.append(f"Description: {_esc(description)}")

            # Règles d'affichage des exemples
            if col.get("is_categorical"):
                parts.append(
                    f"Exemple de 10 valeurs possibles pour ce champ : {_esc((col.get('examples') or '').strip())}"
                )
                # sinon: ne rien afficher (spécification inchangée)
            else:
                examples = (col.get("examples") or "").strip()
                if examples:
                    parts.append(
                        f"Exemples de valeurs possibles pour ce champ : {_esc(examples)}"
                    )

            lines.append(
                "Column name: {name}".format(name=col["name"])
                + (", " + ", ".join(parts) if parts else "")
            )

        if table.get("columns"):
            content.append("\n".join(lines))

    return "\n\n".join(content)


async def get_tables_mapping(project_id: str, **_):
    from models.schemas import get_schemas

    schema = await get_schemas(project_id)
    mapping = {}
    for table in schema:
        cols = {}
        for col in table["columns"]:
            cols = {**cols, col["name"]: col["type"]}
        parts = table["table_name"].split(".")
        key = ".".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
        mapping[key] = cols
    return mapping


async def get_tables_mapping_with_suffix(suffix: str, project_id: str, **_):
    from models.schemas import get_schemas

    schema = await get_schemas(project_id)
    mapping = {}
    for table in schema:
        cols = {}
        for col in table["columns"]:
            cols = {**cols, col["name"]: col["type"]}
        mapping = {
            **mapping,
            table["table_name"].split(".")[-1] + "_" + suffix.replace("-", "_"): cols,
        }
    return mapping
