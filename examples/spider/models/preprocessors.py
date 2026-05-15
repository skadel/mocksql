import re

def clean_ga_sql(sql: str) -> str:
    """
    Préprocesseur pour MockSQL :
    - Remplace les tables avec wildcard (*) par des tables fixes.
    - Neutralise les clauses _TABLE_SUFFIX (insensible à la casse) par 'true'.
    """
    
    # 1. Mapping des remplacements de tables
    # On utilise des patterns simples qui capturent la fin du nom de table
    table_mappings = {
        r'ga_sessions_[a-zA-Z0-9*]+': 'ga_sessions_20170201',
        r'gsod_[a-zA-Z0-9*]+': 'gsod_2019',
        r'bls_qcew\.2018_[a-zA-Z0-9*]+': 'bls_qcew.2018_annual',
    }

    for pattern, replacement in table_mappings.items():
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    # 2. Neutralisation des _TABLE_SUFFIX
    # flags=re.IGNORECASE garantit que _TABLE_SUFFIX, _table_suffix, etc. sont détectés
    # On utilise \s+ pour les espaces et (?:[\'\w]+) pour capturer les valeurs (ex: '0401', start_date)
    sql = re.sub(
        r'_TABLE_SUFFIX\s+(?:BETWEEN\s+[\'\"\w]+\s+AND\s+[\'\"\w]+|=\s*[\'\"\w]+)', 
        'true', 
        sql,
        flags=re.IGNORECASE
    )
        
    return sql