import { createSelector } from '@reduxjs/toolkit';
import { RootState } from '../app/store';
import { Message, MessageGroup } from '../utils/types';

const isMessage = (item: any): item is Message => !!item && item.id !== undefined;

const MERGEABLE_CT = new Set<string>(['stream_part', 'tool_progress', 'partial', 'intermediate']);

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
  },
  sqlPrice: (msg2 as any).sqlPrice ?? (msg1 as any).sqlPrice,
  sqlError: (msg2 as any).sqlError ?? (msg1 as any).sqlError,
  parent: msg1.parent,
  request: msg1.request ?? msg2.request,
  id: msg2.id,
});

/** ========= selector principal ========= */
export const getRenderMessages = createSelector(
  [(state: RootState) => state.buildModel.queryComponentGraph],
  (messages): (Message | MessageGroup)[] => {
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

      const last = out[out.length - 1];

      if (last && isMessage(last)) {
        if (canMerge(last, msg)) {
          out[out.length - 1] = mergeBotMessages(last, msg);
        } else if (
          last.type === 'bot' &&
          msg.type === 'bot' &&
          getNearestUserAncestor(last) === getNearestUserAncestor(msg)
        ) {
          out[out.length - 1] = coalesceBotMessages(last, msg);
        } else {
          out.push(msg);
        }
      } else {
        out.push(msg);
      }

      const children = getChildren(msg.id);
      if (children.length > 1) {
        const branches: (Message | MessageGroup)[][] = [];
        for (const child of children) {
          const branch: (Message | MessageGroup)[] = [];
          processRec(child, branch);
          branches.push(branch);
        }
        out.push({ type: 'group', parentId: msg.id, branches });
      } else if (children.length === 1) {
        processRec(children[0], out);
      }
    };

    if (roots.length > 1) {
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

    return processed;
  }
);
