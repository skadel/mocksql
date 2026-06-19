// Détecte une erreur de "schéma en cache périmé" (colonne référencée dans le SQL
// mais absente du schéma caché). Le backend renvoie ce cas sous deux formes :
//   - banner court : "Unknown column: X (schéma en cache probablement périmé)"
//   - message long : "...schéma en cache **périmé ou incomplet**... refresh-schemas"
// Sur match, l'UI propose un bouton de rafraîchissement du schéma.
export function isStaleSchemaError(error?: string | null): boolean {
  if (!error) return false;
  return /refresh-schemas|en cache.*périmé|périmé.*cache/i.test(error);
}
