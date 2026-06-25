import { DebugRunCteResult, DebugCountStepsResult, DiagnosticBlock, Message, MessageContents, MsgType } from "./types";

export function getLastMessage(messages, selectedChildIndices) {
  if (!messages || messages.length === 0) return null;
  const lastItem = messages[messages.length - 1];

  // 1) Groupe : suivre la branche sélectionnée (ou la dernière par défaut)
  if (lastItem && lastItem.type === 'group') {
    const groupId = lastItem.parentId;
    const selectedIndex =
      (selectedChildIndices && selectedChildIndices[groupId]) ??
      (lastItem.branches.length - 1);
    const safeIndex = Math.max(0, Math.min(selectedIndex, lastItem.branches.length - 1));
    const selectedBranch = lastItem.branches[safeIndex] ?? lastItem.branches[0];
    return getLastMessage(selectedBranch, selectedChildIndices);
  }

  // 2) Bulle de requête : renvoyer le dernier message qu'elle contient
  if (lastItem && lastItem.type === 'request_group') {
    const items = lastItem.items || [];
    return items.length > 0 ? items[items.length - 1] : null;
  }

  // 3) Message simple : renvoyer tel quel
  return lastItem || null;
}


export function formatMessage(message: any): Message {
  const messageContentType =
    message?.additional_kwargs?.type ||
    message?.additional_kwargs?.additional_kwargs?.type;

  const USER_CONTENT_TYPES = ['query', 'examples_update', 'sql_update', 'user_examples', 'provided_sql'];

  const messageType: 'user' | 'bot' =
    USER_CONTENT_TYPES.includes(messageContentType)
      ? 'user'
      : message?.type === 'human'
        ? 'user'
        : 'bot';

  const newMessage: Message = {
    id: message.id || '',
    type: messageType,
    contents: {} as MessageContents,
    children: [],
    parent: message.additional_kwargs?.parent || null,
    request: message.additional_kwargs?.request_id || null,
    contentType: messageContentType || null,
  } as Message & { contentType?: string | null; analysisStep?: number | null };

  switch (messageContentType) {
    case 'examples': {
      const rawTables = JSON.parse(message.content);
      // Generator now emits a single test dict; normalise to array for consumers.
      newMessage.contents.tables = Array.isArray(rawTables) ? rawTables : [rawTables];
      if (message.additional_kwargs?.sql) {
        newMessage.contents.sql = message.additional_kwargs.sql;
      }
      if (message.additional_kwargs?.optimized_sql) {
        newMessage.contents.optimizedSql = message.additional_kwargs.optimized_sql;
      }
      break;
    }

    case 'user_examples':
      newMessage.contents.text = "Modification des exemples";
      newMessage.contents.tables = JSON.parse(message.content);
      break;

    case 'sql_update':
      newMessage.contents.text = "Modification de la requête";
      break;

    case 'error':
      newMessage.contents.error = (message.content === 'unsolvable')
        ? 'Je ne trouves pas les informations nécéssaires pour répondre à la question.'
        : message.content;
      break;

    case 'results':
      try { newMessage.contents.res = JSON.parse(message.content); } catch { /* non-JSON content */ }
      if (message.additional_kwargs?.sql) {
        newMessage.contents.sql = message.additional_kwargs.sql;
      }
      if (message.additional_kwargs?.optimized_sql) {
        newMessage.contents.optimizedSql = message.additional_kwargs.optimized_sql;
      }
      if (message.additional_kwargs?.rerun_all) {
        (newMessage.contents as any).rerunAll = true;
      }
      break;

    case 'evaluation':
      newMessage.contents.text = message.content;
      if (message.additional_kwargs?.test_index !== undefined) {
        newMessage.testIndex = message.additional_kwargs.test_index;
      }
      break;

    case 'delete_test': {
      const parsed = JSON.parse(message.content);
      (newMessage.contents as any).testIndex = parsed.test_index;
      if (message.additional_kwargs?.test_index !== undefined) {
        newMessage.testIndex = message.additional_kwargs.test_index;
      }
      break;
    }

    case 'update_test': {
      const parsed = JSON.parse(message.content);
      (newMessage.contents as any).testIndex = parsed.test_index;
      (newMessage.contents as any).newName = parsed.new_name;
      (newMessage.contents as any).newDescription = parsed.new_description;
      if (message.additional_kwargs?.test_index !== undefined) {
        newMessage.testIndex = message.additional_kwargs.test_index;
      }
      break;
    }

    // Proposition (non appliquée) de mise à jour de description : narration dans le fil ;
    // les boutons Valider/Refuser vivent dans le panneau (état test.proposed_*).
    case 'update_test_proposal': {
      const parsed = JSON.parse(message.content);
      (newMessage.contents as any).testIndex = parsed.test_index;
      (newMessage.contents as any).newName = parsed.new_name;
      (newMessage.contents as any).newDescription = parsed.new_description;
      (newMessage.contents as any).reason = parsed.reason;
      if (message.additional_kwargs?.test_index !== undefined) {
        newMessage.testIndex = message.additional_kwargs.test_index;
      }
      break;
    }

    case 'suggestions': {
      try {
        newMessage.contents.suggestions = JSON.parse(message.content);
      } catch {
        newMessage.contents.suggestions = [];
      }
      newMessage.contents.rationales = message.additional_kwargs?.rationales ?? {};
      newMessage.contents.profileAvailable = message.additional_kwargs?.profile_available ?? true;
      break;
    }

    case 'generate_test_scenario':
      newMessage.contents.text = message.content;
      newMessage.contents.action = message.additional_kwargs?.action ?? 'update';
      break;

    case 'final_response':
      newMessage.contents.text = message.content;
      break;

    case MsgType.QUERY_UNDERSTANDING: {
      try {
        newMessage.contents.understanding = JSON.parse(message.content);
      } catch {
        // unparseable → render nothing (card silently omitted)
      }
      break;
    }

    case 'retry_prompt': {
      if (message.additional_kwargs?.test_index !== undefined) {
        newMessage.testIndex = message.additional_kwargs.test_index;
      }
      break;
    }

    // Le prompt de validation pilote le panneau (via test.reason_type), pas le fil :
    // on n'affiche aucun texte ici (sinon doublon avec le bloc Valider/Corriger).
    case 'validation_prompt': {
      if (message.additional_kwargs?.test_index !== undefined) {
        newMessage.testIndex = message.additional_kwargs.test_index;
      }
      break;
    }

    case MsgType.DEBUG_RUN_CTE: {
      try {
        newMessage.contents.debugRunCte = JSON.parse(message.content) as DebugRunCteResult;
      } catch {
        newMessage.contents.error = message.content;
      }
      break;
    }

    case MsgType.DEBUG_COUNT_STEPS: {
      try {
        newMessage.contents.debugCountSteps = JSON.parse(message.content) as DebugCountStepsResult;
      } catch {
        newMessage.contents.error = message.content;
      }
      break;
    }

    case MsgType.BAD_DATA_DIAGNOSTIC: {
      try {
        newMessage.contents.diagnostic = JSON.parse(message.content) as DiagnosticBlock;
      } catch {
        // no text fallback — message is intentionally silent if unparseable
      }
      if (message.additional_kwargs?.test_index !== undefined) {
        newMessage.testIndex = message.additional_kwargs.test_index;
      }
      break;
    }

    default: {
      const raw = message.content;
      if (Array.isArray(raw)) {
        console.warn('[formatMessage] content is array for type', messageContentType, raw);
        newMessage.contents.text = raw
          .filter((c: any) => c?.type === 'text')
          .map((c: any) => c?.text ?? '')
          .join('');
      } else {
        newMessage.contents.text = raw;
      }
      break;
    }
  }

  return newMessage;
}
