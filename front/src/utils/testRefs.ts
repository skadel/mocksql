/**
 * Liens cliquables vers les tests dans les réponses du chat.
 *
 * Le conversational_agent (backend) référence un test par sa « réf. outil » (test_uid)
 * sous la forme `[[test:UID]]` — un identifiant technique que l'utilisateur ne voit jamais
 * à l'écran. Côté front, on remplace ce marqueur par un lien markdown « test N » (N = rang
 * d'écran) que MessageBody rend en chip cliquable menant à la carte du test.
 *
 * On encode le rang dans un href de type ancre (`#mocksql-test-N`) plutôt qu'un schéma
 * d'URL custom : react-markdown v9 conserve les ancres (pas de protocole à filtrer), alors
 * qu'il viderait un href `mocksql-test:N`.
 */

/** Marqueur émis par le LLM : `[[test:88eb]]`. */
export const TEST_REF_RE = /\[\[test:([0-9a-zA-Z]+)\]\]/g;

/** Préfixe d'ancre porté par les liens de test générés (cf. `linkifyTestRefs`). */
export const TEST_REF_HASH_PREFIX = '#mocksql-test-';

type TestLike = { test_uid?: string | null };

/** Construit la table `test_uid → rang d'écran (1-based)` depuis la liste ordonnée des tests. */
export function buildTestUidIndex(tests: TestLike[] | undefined | null): Record<string, number> {
  const map: Record<string, number> = {};
  (tests || []).forEach((t, i) => {
    const uid = t?.test_uid;
    if (uid) map[uid] = i + 1;
  });
  return map;
}

/**
 * Remplace les marqueurs `[[test:UID]]` par des liens markdown « test N ».
 *
 * Un uid inconnu (test supprimé, réf. hallucinée) retombe sur un simple « test » en clair —
 * jamais l'uid brut, que l'utilisateur ne reconnaîtrait pas.
 */
export function linkifyTestRefs(text: string, uidToIndex: Record<string, number>): string {
  if (!text) return text;
  return text.replace(TEST_REF_RE, (_match, uid: string) => {
    const n = uidToIndex[uid];
    if (!n) return 'test';
    return `[test ${n}](${TEST_REF_HASH_PREFIX}${n})`;
  });
}

/** Vrai si l'href provient d'un marqueur de test linkifié. */
export function isTestRefHref(href: string | undefined): boolean {
  return !!href && href.startsWith(TEST_REF_HASH_PREFIX);
}

/** Extrait le rang d'écran (1-based) d'un href de test, ou `null`. */
export function testRankFromHref(href: string | undefined): number | null {
  if (!isTestRefHref(href)) return null;
  const n = parseInt((href as string).slice(TEST_REF_HASH_PREFIX.length), 10);
  return Number.isFinite(n) ? n : null;
}
