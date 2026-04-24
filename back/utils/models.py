from typing import Optional, List

from pydantic import BaseModel, Field


class SubQuestion(BaseModel):
    sub_question: str = Field(
        description="Une sous-question qui aide à décomposer le problème principal."
    )
    sub_question_name: str = Field(
        description="Le nom de la vue correspondant à la sous-question."
    )
    analysis: str = Field(
        description="Une version enrichie de la sous-question expliquant les éléments clés pour une meilleure "
        "compréhension, exprimée en langage métier."
    )
    sub_query: str = Field(
        description="Une sous-requête SQL __dialect__ valide correspondant à la sous-question, incluant les "
        "pré-traitements si nécessaire."
    )


class DivideAndConquer(BaseModel):
    main_question: str = Field(description="La question principale de l'analyse.")
    query_name: str = Field(description="Nom court de la requête")
    purpose: str = Field(description="Objectif final de la requête.")
    question_enriched: str = Field(
        description="Version enrichie de la question principale pour une meilleure compréhension, formulée en langage "
        "métier."
    )

    requires_split: bool = Field(
        description="Indique si la requête nécessite d'être décomposée en plusieurs sous-questions."
    )
    sub_questions: Optional[List[SubQuestion]] = Field(
        default=None,
        description="Liste de sous-questions, chacune avec ses sous-requêtes correspondantes. Non utilisé si la requête"
        " est simple.",
    )
    unsolvable: bool = Field(
        description="Indique si la demande ne peut pas être résolue."
    )
    final_sql: str = Field(
        description="Requête SQL __dialect__ finale qui utilise les sous-requêtes précédentes pour construire le code "
        "SQL final. Ne pas réécrire les autres CTEs. Ce code ne doit pas contenir les CTEs précédément "
        "définies."
    )


class ExampleBase(BaseModel):
    query_name: str = Field(..., description="Le nom de la requête (Question name).")
    main_question: str = Field(..., description="La question principale de la requête.")
    purpose: Optional[str] = Field(None, description="L'objectif de l'exemple.")
    question_enriched: Optional[str] = Field(
        None, description="L'explication/enrichissement."
    )
    sql: Optional[str] = Field(None, description="La requête SQL (mise à jour).")
    final_sql: Optional[str] = Field(
        None, description="Le select final de la requete SQL."
    )
    ctes: Optional[str] = Field(None, description="list of cte and their explanations.")


class ExampleCreate(ExampleBase):
    """
    Schéma pour créer un nouvel exemple.
    Hérite de ExampleBase (tous les champs obligatoires/optionnels décrits là).
    """

    pass


class ExampleUpdate(ExampleBase):
    """
    Schéma pour mettre à jour un exemple existant.
    Aucun `id` ici : l'identifiant vient de l'URL (/api/examples/{example_id}).
    Tous les champs sont optionnels : on peut PATCH/PUT partiellement.
    """

    id: str = Field(..., description="L'ID de l'exemple à mettre à jour.")
    query_name: Optional[str] = Field(
        None, description="Le nom de la requête (mise à jour)."
    )
    main_question: Optional[str] = Field(
        None, description="Le contenu/texte (mise à jour)."
    )
    purpose: Optional[str] = Field(None, description="L'objectif (mise à jour).")
    question_enriched: Optional[str] = Field(
        None, description="L'explication enrichie (mise à jour)."
    )
    sql: Optional[str] = Field(None, description="La requête SQL (mise à jour).")
    final_sql: Optional[str] = Field(
        None, description="Le select final de la requete SQL."
    )
    ctes: Optional[str] = Field(None, description="list of cte and their explanations.")


class ExampleOut(ExampleBase):
    """
    Schéma retourné au frontend après création ou mise à jour d'un exemple.
    On y ajoute l'`id` et le `project_id`.
    """

    id: str = Field(..., description="Identifiant unique de l'exemple.")


# Modèle pour la modification ciblée de requêtes existantes
class ModifyDivideAndConquer(BaseModel):
    main_question: str = Field(
        description="La question ajusté après modification ou extension."
    )
    query_name: str = Field(description="Nom court de la requête pour référence.")
    purpose: str = Field(description="Objectif ajusté après modification ou extension.")
    question_enriched: str = Field(
        description="Contexte métier enrichi, ciblant les éléments à ajuster ou à ajouter."
    )
    requires_split: bool = Field(
        description="Indique si une partie doit être découplée ou étendue."
    )
    sub_questions: Optional[List["ModifySubQuestion"]] = Field(
        default=None, description="Sous-questions à créer ou mettre à jour."
    )
    unsolvable: bool = Field(
        description="Indique si aucune modification n'est possible."
    )
    final_sql: str = Field(
        description="Requête SQL __dialect__ finale avec modifications."
    )


class ModifySubQuestion(BaseModel):
    sub_question: str = Field(
        description="Une sous-question, existante ou nouvelle, ciblant une étape précise du traitement SQL."
    )
    sub_question_name: str = Field(description="Nom de la vue pour la sous-question.")
    analysis: str = Field(
        description="Analyse des changements requis; si aucun, mettre 'pas de changement'."
    )
    sub_query: str = Field(
        description="La sous-requête SQL __dialect__ à modifier, optimiser ou créer."
    )
