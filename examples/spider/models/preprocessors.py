import re


def clean_ga_sql(sql: str) -> str:
    """
    Préprocesseur pour MockSQL :
    - Remplace les tables avec wildcard (*) par des tables fixes.
    - Neutralise les clauses _TABLE_SUFFIX (insensible à la casse) par 'true'.
    """

    table_mappings = {
        r"ga_sessions_[a-zA-Z0-9*]+": "ga_sessions_20170201",
        r"gsod_[a-zA-Z0-9*]+": "gsod_2019",
        r"bls_qcew\.2018_[a-zA-Z0-9*]+": "bls_qcew.2018_annual",
    }

    for pattern, replacement in table_mappings.items():
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    sql = re.sub(
        r"_TABLE_SUFFIX\s+(?:BETWEEN\s+['\"\w]+\s+AND\s+['\"\w]+|=\s*['\"\w]+)",
        "true",
        sql,
        flags=re.IGNORECASE,
    )

    return sql
