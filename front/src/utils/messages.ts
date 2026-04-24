import { Message, MessageContents, MsgType } from "./types";

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

  // 2) Message simple : renvoyer tel quel
  return lastItem || null;
}


export function formatMessage(message: any): Message {
  const messageContentType =
    message?.additional_kwargs?.type ||
    message?.additional_kwargs?.additional_kwargs?.type;

  const messageType: 'user' | 'bot' =
    message?.type === 'human' ? 'user'
    : message?.type === 'ai' ? 'bot'
    : (['query', 'examples_update', 'sql_update', 'user_examples', 'provided_sql']
        .includes(messageContentType)
        ? 'user'
        : 'bot');

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
    case MsgType.PROFILE_QUERY: {
      const parsed = JSON.parse(message.content);
      newMessage.contents.profileRequest = {
        message: parsed.message,
        profile_query: parsed.profile_query,
        missing_columns: parsed.missing_columns,
      };
      break;
    }

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
      newMessage.contents.res = JSON.parse(message.content);
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

    default:
      newMessage.contents.text = message.content;
      break;
  }

  return newMessage;
}
