import { createSelector } from '@reduxjs/toolkit';
import { RootState } from '../app/store';
import { AnyRenderable, Message, MessageGroup, MsgType } from '../utils/types';

const DEBUG_TYPES = new Set<string>([MsgType.DEBUG_RUN_CTE, MsgType.DEBUG_COUNT_STEPS]);

const isMessage = (item: any): item is Message => !!item && item.id !== undefined && item.type !== 'group' && item.type !== 'request_group';

/**
 * Types d'étapes intermédiaires : repliés dans la bulle de requête.
 * Tout le reste (réponse finale, suggestions, texte libre, erreur) reste visible.
 */
const STEP_CONTENT_TYPES = new Set<string>([
  'generate_test_scenario',
  'examples',
  'results',
  'evaluation',
  'bad_data_diagnostic',
  'update_test',
  'delete_test',
  MsgType.DEBUG_RUN_CTE,
  MsgType.DEBUG_COUNT_STEPS,
]);

/** Un message est une « étape » (repliée) s'il représente un travail intermédiaire. */
export const isStepMessage = (m: Message): boolean => {
  const ct = ((m as any).contentType as string | undefined) ?? '';
  if (STEP_CONTENT_TYPES.has(ct)) return true;
  const c = m.contents || {};
  return (
    (Array.isArray(c.tables) && c.tables.length > 0) ||
    (Array.isArray(c.res) && c.res.length > 0) ||
    !!c.diagnostic ||
    !!c.debugRunCte ||
    !!c.debugCountSteps
  );
};

const MERGEABLE_CT = new Set<string>(['stream_part', 'tool_progress', 'partial', 'intermediate']);

// These types are always rendered as standalone bubbles — never merged into adjacent messages.
const NON_COALESCING_TYPES = new Set<string>(['suggestions', 'evaluation', 'generate_test_scenario', 'update_test_proposal']);

const getContentType = (m: Message): string | null =>
  ((m as any).contentType as string | undefined) ?? null;

/** ========= fusions "stream/partials" (même contentType, mergeable) ========= */
const canMerge = (a: Message, b: Message) => {
  if (a.type !== 'bot' || b.type !== 'bot') return false;
  const act = getContentType(a);
  const bct = getContentType(b);
  return !!act && act === bct && MERGEABLE_CT.has(act);
};

const mergeBotMessages = (msg1: Message, msg2: Message): Message => ({
  ...msg2,
  contents: {
    text: msg2.contents.text ?? msg1.contents.text,
    sql: msg2.contents.sql ?? msg1.contents.sql,
    tables: msg2.contents.tables ?? msg1.contents.tables,
    res: msg2.contents.res ?? msg1.contents.res,
    meta: msg2.contents.meta ?? msg1.contents.meta,
    real_res: msg2.contents.real_res ?? msg1.contents.real_res,
    error: msg2.contents.error ?? msg1.contents.error,
  },
  sqlPrice: (msg2 as any).sqlPrice ?? (msg1 as any).sqlPrice,
  sqlError: (msg2 as any).sqlError ?? (msg1 as any).sqlError,
  parent: msg1.parent,
  request: msg1.request ?? msg2.request,
  id: msg2.id,
});

/** ========= helpers pour le grouping ========= */
const concatMaybeArray = <T>(a: any, b: any): T[] | undefined => {
  const aa = Array.isArray(a) ? a : undefined;
  const bb = Array.isArray(b) ? b : undefined;
  if (aa && bb) return [...aa, ...bb];
  return bb ?? aa ?? undefined;
};

const mergeRecordOfArrays = (
  a?: Record<string, any[]>,
  b?: Record<string, any[]>
): Record<string, any[]> | undefined => {
  if (!a && !b) return undefined;
  if (!a) return b;
  if (!b) return a;
  const out: Record<string, any[]> = { ...a };
  for (const k of Object.keys(b)) {
    const av = a[k];
    const bv = b[k];
    if (Array.isArray(av) && Array.isArray(bv)) out[k] = [...av, ...bv];
    else if (Array.isArray(bv)) out[k] = bv;
    else if (Array.isArray(av)) out[k] = av;
  }
  return out;
};

const joinText = (t1?: any, t2?: any): string | undefined => {
  const a = (typeof t1 === 'string' ? t1 : '').trim();
  const b = (typeof t2 === 'string' ? t2 : '').trim();
  if (a && b) return `${a}\n\n${b}`;
  return b || a || undefined;
};

const coalesceBotMessages = (msg1: Message, msg2: Message): Message => ({
  ...msg2,
  contents: {
    text: joinText(msg1.contents?.text, msg2.contents?.text),
    sql: msg2.contents?.sql ?? msg1.contents?.sql,
    tables: mergeRecordOfArrays(
      msg1.contents?.tables as Record<string, any[]> | undefined,
      msg2.contents?.tables as Record<string, any[]> | undefined
    ),
    res: concatMaybeArray(msg1.contents?.res, msg2.contents?.res),
    real_res: concatMaybeArray(msg1.contents?.real_res, msg2.contents?.real_res),
    meta: msg2.contents?.meta ?? msg1.contents?.meta,
    error: msg2.contents?.error ?? msg1.contents?.error,
    suggestions: msg2.contents?.suggestions ?? msg1.contents?.suggestions,
  },
  sqlPrice: (msg2 as any).sqlPrice ?? (msg1 as any).sqlPrice,
  sqlError: (msg2 as any).sqlError ?? (msg1 as any).sqlError,
  parent: msg1.parent,
  request: msg1.request ?? msg2.request,
  id: msg2.id,
});

/**
 * Post-passe : regroupe les suites de messages bot d'une même requête (request_id)
 * en une bulle unique RequestGroup, à condition qu'au moins une étape intermédiaire
 * existe (sinon on laisse le rendu linéaire). Récursif dans les branches de groupes.
 */
const bundleRequests = (items: AnyRenderable[]): AnyRenderable[] => {
  const out: AnyRenderable[] = [];
  let run: Message[] = [];

  const flushRun = () => {
    if (run.length === 0) return;
    const hasStep = run.some(isStepMessage);
    if (run.length >= 2 && hasStep) {
      out.push({ type: 'request_group', requestId: run[0].request || '', items: run });
    } else {
      out.push(...run);
    }
    run = [];
  };

  for (const item of items) {
    if ('type' in item && (item as any).type === 'group') {
      flushRun();
      const group = item as MessageGroup;
      out.push({
        ...group,
        branches: group.branches.map(bundleRequests),
      });
      continue;
    }
    const msg = item as Message;
    const reqId = msg.request;
    // Les erreurs restent des bulles autonomes : leur carte « Corriger » est rendue
    // au niveau de MessageDisplay, pas dans le corps groupé.
    const canGroup = msg.type === 'bot' && !!reqId && !msg.contents?.error;
    if (canGroup && (run.length === 0 || run[run.length - 1].request === reqId)) {
      run.push(msg);
    } else {
      flushRun();
      if (canGroup) {
        run.push(msg);
      } else {
        out.push(msg);
      }
    }
  }
  flushRun();
  return out;
};

/** ========= selector principal ========= */
export const getRenderMessages = createSelector(
  [(state: RootState) => state.buildModel.queryComponentGraph],
  (messages): AnyRenderable[] => {
    const processed: (Message | MessageGroup)[] = [];
    const processedIds = new Set<string>();
    const parentToChildren: Record<string, Message[]> = {};
    const roots: Message[] = [];

    // construire parent→enfants + racines
    Object.values(messages)
      .filter(isMessage)
      .forEach((msg: Message) => {
        if (msg.parent && msg.parent !== '' && msg.parent in messages) {
          (parentToChildren[msg.parent] ||= []).push(msg);
        } else {
          roots.push(msg);
        }
      });

    // util : ancêtre user le plus proche
    const userAncestorMemo = new Map<string, string | null>();
    const getNearestUserAncestor = (m: Message): string | null => {
      if (userAncestorMemo.has(m.id)) return userAncestorMemo.get(m.id)!;
      let cur: Message | undefined = m;
      const visited = new Set<string>();
      while (cur && cur.parent && !visited.has(cur.id)) {
        visited.add(cur.id);
        const p = (messages as any)[cur.parent] as Message | undefined;
        if (!p) break;
        if (p.type === 'user') {
          userAncestorMemo.set(m.id, p.id);
          return p.id;
        }
        cur = p;
      }
      userAncestorMemo.set(m.id, null);
      return null;
    };

    const getChildren = (id: string) => parentToChildren[id] || [];

    const processRec = (msg: Message, out: (Message | MessageGroup)[]) => {
      if (processedIds.has(msg.id)) return;
      processedIds.add(msg.id);

      // Debug messages are embedded inside their parent as a collapsed section, not standalone.
      if (DEBUG_TYPES.has(getContentType(msg) ?? '')) return;

      // sql_update messages are already represented by SqlChangeDivider — skip the bubble
      // but still recurse into children so bot responses are rendered.
      if (getContentType(msg) === 'sql_update') {
        const children = getChildren(msg.id).filter(c => !DEBUG_TYPES.has(getContentType(c) ?? ''));
        if (children.length > 1) {
          const hasUserChild = children.some(c => c.type === 'user');
          if (hasUserChild) {
            const branches: (Message | MessageGroup)[][] = [];
            for (const child of children) {
              const branch: (Message | MessageGroup)[] = [];
              processRec(child, branch);
              branches.push(branch);
            }
            out.push({ type: 'group', parentId: msg.id, branches });
          } else {
            for (const child of children) processRec(child, out);
          }
        } else if (children.length === 1) {
          processRec(children[0], out);
        }
        return;
      }

      const last = out[out.length - 1];

      if (last && isMessage(last)) {
        if (canMerge(last, msg)) {
          out[out.length - 1] = mergeBotMessages(last, msg);
        } else if (
          last.type === 'bot' &&
          msg.type === 'bot' &&
          !NON_COALESCING_TYPES.has(getContentType(msg) ?? '') &&
          !NON_COALESCING_TYPES.has(getContentType(last) ?? '') &&
          getNearestUserAncestor(last) === getNearestUserAncestor(msg)
        ) {
          out[out.length - 1] = coalesceBotMessages(last, msg);
        } else {
          out.push(msg);
        }
      } else {
        out.push(msg);
      }

      // Filter out debug children — they are rendered inside the parent.
      const children = getChildren(msg.id).filter(
        c => !DEBUG_TYPES.has(getContentType(c) ?? '')
      );
      if (children.length > 1) {
        // Create a MessageGroup only when children include user messages (true branching from edits).
        // When all children are bot messages they are sequential responses — render them linearly
        // so that the coalescing/non-coalescing rules above apply uniformly to SSE and reload.
        const hasUserChild = children.some(c => c.type === 'user');
        if (hasUserChild) {
          const branches: (Message | MessageGroup)[][] = [];
          for (const child of children) {
            const branch: (Message | MessageGroup)[] = [];
            processRec(child, branch);
            branches.push(branch);
          }
          out.push({ type: 'group', parentId: msg.id, branches });
        } else {
          for (const child of children) {
            processRec(child, out);
          }
        }
      } else if (children.length === 1) {
        processRec(children[0], out);
      }
    };

    if (roots.length > 1) {
      // Only create a root-level group when there are truly alternative conversation threads
      // (multiple user-type roots = the user sent different first messages).
      // Orphaned bot roots (e.g. evaluation messages whose parent link was lost on reload)
      // are treated as sequential items rendered linearly.
      const userRoots = roots.filter(r => r.type === 'user');
      if (userRoots.length > 1) {
        const rootBranches: (Message | MessageGroup)[][] = [];
        for (const r of roots) {
          const br: (Message | MessageGroup)[] = [];
          processRec(r, br);
          rootBranches.push(br);
        }
        processed.push({ type: 'group', parentId: 'root_message', branches: rootBranches });
      } else {
        roots.forEach(r => processRec(r, processed));
      }
    } else {
      roots.forEach(r => processRec(r, processed));
    }

    return bundleRequests(processed);
  }
);
