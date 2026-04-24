import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { chatQuery, fetchPage } from '../../api/query';
import { fetchUniqueColumns, getTableChanges } from '../../api/table';
import { getMessages, patchModelTests } from '../../api/messages';
import { handleRejectedCase } from '../../utils/errorCase';
import { BuildModelState, DisplayTableMeta, Message, SqlHistoryEntry } from '../../utils/types';
import { formatMessage } from '../../utils/messages';

const initialState: BuildModelState = {
  activeStep: 0,
  selectedDatabases: [],
  error: null,
  loading: null,
  name: '',
  validateTestSuccess: false,
  validateTestLoading: false,
  queryComponentMessages: [],
  queryComponentGraph: {},
  testResults: [],
  lastUserInput: '',
  uniqueColumns: [],
  schemaDiff: {
    added: [],
    removed: [],
    modified: {}
  },
  joinedSample: '',
  devOnlySample: '',
  prodOnlySample: '',
  selectedChildIndices: {},
  sqlHistory: [],
  restoredMessageId: undefined,
  lastError: undefined,
};

// Fonction utilitaire pour remonter du message jusqu'à la racine
function findPathToRoot(state: BuildModelState, messageId: string): string[] {
  const path: string[] = [];
  let currentId = messageId;
  while (currentId) {
    path.push(currentId);
    const currentMessage = state.queryComponentGraph[currentId];
    if (currentMessage && currentMessage.parent) {
      currentId = currentMessage.parent;
    } else {
      break;
    }
  }
  return path;
}

// Fonction pour mettre à jour selectedChildIndices
function setDefaultBranchSelection(state: BuildModelState, lastMessageId: string) {
  let path = findPathToRoot(state, lastMessageId);
  // Inverser pour obtenir le chemin de la racine vers le dernier message
  path = path.reverse();
  for (let i = 0; i < path.length - 1; i++) {
    const parentId = path[i];
    const childId = path[i + 1];
    const parentMessage = state.queryComponentGraph[parentId];
    if (parentMessage && parentMessage.children) {
      const index = parentMessage.children.indexOf(childId);
      if (index >= 0) {
        state.selectedChildIndices[parentId] = index;
      }
    }
  }
}

export const buildModelSlice = createSlice({
  name: 'buildModel',
  initialState,
  reducers: {
    setActiveStep: (state, action: PayloadAction<number>) => {
      state.activeStep = action.payload;
    },
    setSelectedDatabases: (state, action: PayloadAction<string[]>) => {
      state.selectedDatabases = action.payload;
    },
    setError: (state, action: PayloadAction<string | null>) => {
      state.error = action.payload;
    },
    setValidateDataSuccess: (state, action: PayloadAction<boolean>) => {
      state.validateTestSuccess = action.payload;
    },
    setLoadingTestDataSuccess: (state, action: PayloadAction<boolean>) => {
      state.validateTestLoading = action.payload;
    },
    setQueryComponentGraph: (state, action: PayloadAction<Record<string, Message>>) => {
      state.queryComponentGraph = action.payload;
    },
    appendComponentToLastMessage(state, action: PayloadAction<Message>) {
      let newMessage = false;
      let targetMessageKey: string | null = null;

      // Find the first message with no id or id === '' that has the same parent as the payload
      for (const key in state.queryComponentGraph) {
        const message = state.queryComponentGraph[key];
        if (
          (message.id === action.payload.id || !message.id) &&
          message.parent === action.payload.parent
        ) {
          targetMessageKey = key;
          break; // Found the first matching message
        }
      }

      if (targetMessageKey !== null) {
        const targetMessage = state.queryComponentGraph[targetMessageKey];

        // Merge contents by appending 'text' and 'sql'
        const mergedContents = { ...targetMessage.contents };

        for (const key in action.payload.contents) {
          if (key === 'text') {
            // Concatenate texts if both exist
            if (mergedContents.text) {
              mergedContents.text += action.payload.contents.text;
            } else {
              mergedContents.text = action.payload.contents.text;
            }
          } else if (key === 'sql') {
            // Concatenate SQL if both exist
            if (mergedContents.sql) {
              mergedContents.sql += action.payload.contents.sql;
            } else {
              mergedContents.sql = action.payload.contents.sql;
            }
          }
        }

        targetMessage.contents = mergedContents;

      } else {
        newMessage = true;
      }

      if (newMessage) {
        // No matching message found, so add as a new message
        const messageId =
          action.payload.id ? action.payload.id : '';
        state.queryComponentGraph[messageId] = action.payload;
      }
    },
    appendQueryComponentMessage(state, action: PayloadAction<Message>) {
      const msg = action.payload;
      state.queryComponentGraph[msg.id] = msg;

      if (msg.parent && state.queryComponentGraph[msg.parent]) {
        const parentMessage = state.queryComponentGraph[msg.parent];
        parentMessage.children = parentMessage.children || [];
        parentMessage.children.push(msg.id);
      }

      // Keep state.testResults in sync as results arrive via SSE.
      // rerun_all → replace everything; otherwise merge by test_index.
      if (Array.isArray(msg.contents.res)) {
        const isRerunAll = (msg.contents as any).rerunAll;
        if (isRerunAll) {
          state.testResults = msg.contents.res as any[];
        } else {
          const existing = [...(state.testResults || [])];
          (msg.contents.res as any[]).forEach((newR: any) => {
            const idx = existing.findIndex((r: any) => r.test_index === newR.test_index);
            if (idx >= 0) existing[idx] = newR;
            else existing.push(newR);
          });
          state.testResults = existing;
        }
      }

      // Pre-populate testResults with pending state as soon as generator examples arrive.
      // Generator now emits one test at a time → merge instead of replace.
      // sql_update context means all existing tests will be re-run → mark them all pending first.
      if (msg.contentType === 'examples' && Array.isArray(msg.contents.tables) && (msg.contents.tables as any[]).length > 0) {
        const newTests = (msg.contents.tables as any[]).map((t: any) => ({ ...t, status: 'pending' }));
        const base = msg.context === 'sql_update'
          ? (state.testResults || []).map((t: any) => ({ ...t, status: 'pending' }))
          : [...(state.testResults || [])];
        newTests.forEach((newT: any) => {
          const idx = base.findIndex((r: any) => r.test_index === newT.test_index);
          if (idx >= 0) base[idx] = newT;
          else base.push(newT);
        });
        state.testResults = base;
      }

      // Attach LLM evaluation verdict to the matching test
      if (msg.contentType === 'evaluation' && msg.contents.text && state.testResults?.length) {
        const testIdx = msg.testIndex;
        state.testResults = state.testResults.map((t: any, i: number) => {
          const matches = testIdx !== undefined ? t.test_index === testIdx : i === state.testResults!.length - 1;
          return matches ? { ...t, evaluation: msg.contents.text } : t;
        });
      }
    },
    removeMessage(state, action: PayloadAction<string>) {
      const tempMessageId = action.payload;

      // Check if the temp_message exists in the graph
      if (state.queryComponentGraph[tempMessageId]) {
        const tempMessage = state.queryComponentGraph[tempMessageId];

        // If temp_message has a parent
        if (tempMessage.parent && state.queryComponentGraph[tempMessage.parent]) {
          const parentMessage = state.queryComponentGraph[tempMessage.parent];

          // Remove tempMessageId from parentMessage.children
          if (parentMessage.children) {
            parentMessage.children = parentMessage.children.filter(
              (childId) => childId !== tempMessageId
            );
          }
        }

        // Remove temp_message from the graph
        delete state.queryComponentGraph[tempMessageId];
      }
    },
    setTransformationName: (state, action: PayloadAction<string>) => {
      state.name = action.payload
    },
    resetContext: (state) => {
      return initialState;
    },
    setStep: (state, action: PayloadAction<string>) => {
      state.step = action.payload;
    },
    setLoading: (state, action: PayloadAction<boolean>) => {
      state.loading = action.payload;
    },
    setQuery: (state, action: PayloadAction<string>) => {
      state.query = action.payload;
    },
    setOptimizedQuery: (state, action: PayloadAction<string>) => {
      state.optimizedQuery = action.payload;
    },
    setUserInput: (state, action: PayloadAction<string>) => {
      state.lastUserInput = action.payload;
    },
    addTextMessage(state, action: PayloadAction<Message>) {
      const msg = action.payload;
      state.queryComponentGraph[msg.id] = msg;
      if (msg.parent && state.queryComponentGraph[msg.parent]) {
        const parent = state.queryComponentGraph[msg.parent];
        parent.children = parent.children || [];
        if (!parent.children.includes(msg.id)) {
          parent.children.push(msg.id);
        }
      }
    },
    resetMessages(state, content: PayloadAction) {
      state.queryComponentMessages = [];
      state.queryComponentGraph = {};
    },
    setLoadingMessage: (state, action: PayloadAction<string | undefined>) => {
      state.loading_message = action.payload;
    },
    setSelectedChildIndex: (state, action) => {
      const { parentId, index } = action.payload;

      if (index === undefined || index === null) {
        // Unset la sélection si index est undefined ou null
        delete state.selectedChildIndices[parentId];
      } else {
        // Sinon, affecte la nouvelle valeur
        state.selectedChildIndices[parentId] = index;
      }
    },
    setTestResults(state, action: PayloadAction<any[]>) {
      state.testResults = action.payload;
    },
    pushSqlHistory(state, action: PayloadAction<SqlHistoryEntry>) {
      const last = state.sqlHistory[state.sqlHistory.length - 1];
      if (!last || last.sql !== action.payload.sql) {
        state.sqlHistory.push(action.payload);
      }
    },
    setRestoredMessageId(state, action: PayloadAction<string | undefined>) {
      state.restoredMessageId = action.payload;
    },
    appendStreamingReasoning(state, action: PayloadAction<string>) {
      state.streamingReasoning = (state.streamingReasoning || '') + action.payload;
    },
    clearStreamingReasoning(state) {
      state.streamingReasoning = undefined;
    },
  },
  extraReducers: (builder) => {
    builder.addCase(getMessages.pending, (state) => {
      state.loading = true;
      state.loading_message = undefined;
      state.error = '';
      state.queryComponentGraph = {};
      state.query = undefined;
      state.optimizedQuery = undefined;
      state.sqlHistory = [];
      state.restoredMessageId = undefined;
      state.lastError = undefined;
    })
      .addCase(getMessages.fulfilled, (state, action: PayloadAction<{ messages: any[]; sql: string | null; optimized_sql: string | null; test_results: any[]; restored_message_id?: string | null; last_error?: string | null; sql_history?: SqlHistoryEntry[] }>) => {
        const { messages, sql, optimized_sql, test_results, restored_message_id, last_error, sql_history } = action.payload;
        state.error = '';
        state.loading = false;
        if (sql) state.query = sql;
        if (optimized_sql) state.optimizedQuery = optimized_sql;
        if (test_results?.length) state.testResults = test_results;
        if (restored_message_id) state.restoredMessageId = restored_message_id;
        state.lastError = last_error || undefined;
        if (sql_history?.length) state.sqlHistory = sql_history;
        // 1. Insérer tous les messages dans queryComponentGraph
        messages.forEach((msg: any) => {
          const newMessage = formatMessage(msg);
          state.queryComponentGraph[newMessage.id] = newMessage;
        });
        // 2. Mettre à jour les relations parent/enfants
        messages.forEach((msg: any) => {
          const newMessage = state.queryComponentGraph[msg.id];
          if (newMessage.parent && state.queryComponentGraph[newMessage.parent]) {
            const parentMessage = state.queryComponentGraph[newMessage.parent];
            parentMessage.children = parentMessage.children || [];
            parentMessage.children.push(newMessage.id);
          }
        });

        // Fallback: find last results message for testResults / sql if model had no persisted data
        if (!test_results?.length || !sql) {
          for (let i = messages.length - 1; i >= 0; i--) {
            const raw = messages[i];
            if (raw?.additional_kwargs?.type === 'results') {
              const processed = state.queryComponentGraph[raw.id];
              if (!test_results?.length && processed && Array.isArray(processed.contents.res) && processed.contents.res.length > 0) {
                state.testResults = processed.contents.res;
              }
              if (!sql && processed?.contents.sql) {
                state.query = processed.contents.sql;
              }
              if (!optimized_sql && processed?.contents.optimizedSql) {
                state.optimizedQuery = processed.contents.optimizedSql;
              }
              if (state.testResults?.length && state.query) break;
            }
          }
        }

        // Attach LLM evaluation verdicts from history to testResults
        if (state.testResults?.length) {
          messages.forEach((raw: any) => {
            if (raw?.additional_kwargs?.type === 'evaluation' && raw.content) {
              const testIdx = raw.additional_kwargs?.test_index;
              state.testResults = state.testResults!.map((t: any, i: number) => {
                const matches = testIdx !== undefined ? t.test_index === testIdx : i === state.testResults!.length - 1;
                return matches ? { ...t, evaluation: raw.content } : t;
              });
            }
          });
        }

        // 4. Déterminer le dernier message et mettre à jour selectedChildIndices
        const latestMessage = messages[messages.length - 1];
        if (latestMessage) {
          setDefaultBranchSelection(state, latestMessage.id);
        }

        state.selectedDatabases = [];
      })
      .addCase(chatQuery.pending, (state) => {
        state.loading = true;
        state.streamingReasoning = undefined;
      })
      .addCase(chatQuery.fulfilled, (state) => {
        console.log("chatquery fulfilled")
        state.loading = false;
        state.loading_message = undefined;
        state.streamingReasoning = undefined;
      })
      .addCase(fetchUniqueColumns.pending, (state) => {
        state.loading = true;
      })
      .addCase(fetchUniqueColumns.fulfilled, (state, action) => {
        state.loading = false;
        state.uniqueColumns = action.payload;
      })
      .addCase(getTableChanges.pending, (state) => {
        state.loading = true;
        state.loading_message = "Create table";
        state.error = '';
        state.schemaDiff = {
          added: [],
          removed: [],
          modified: {}
        };
        state.joinedSample = '';
        state.devOnlySample = '';
        state.prodOnlySample = '';
      })
      .addCase(getTableChanges.fulfilled, (state, action) => {
        state.loading = false;
        state.loading_message = undefined;

        const { schema_diff, joined_sample, dev_only_sample, prod_only_sample } = action.payload;
        state.schemaDiff = schema_diff || state.schemaDiff;
        state.joinedSample = joined_sample || state.joinedSample;
        state.devOnlySample = dev_only_sample || state.devOnlySample;
        state.prodOnlySample = prod_only_sample || state.prodOnlySample;
      })
      .addCase(fetchUniqueColumns.rejected, (state, action) => {
        handleRejectedCase(state, action, "Execution error");
      })
      .addCase(getTableChanges.rejected, (state, action) => {
        handleRejectedCase(state, action, "Execution error");
      })
      .addCase(chatQuery.rejected, (state, action) => {
        handleRejectedCase(state, action, "Execution error");
      })
      .addCase(getMessages.rejected, (state, action) => {
        state.loading = false;
        state.loading_message = undefined;
        const detail =
          action.payload && typeof action.payload === 'object' && 'detail' in action.payload
            ? (action.payload as any).detail
            : action.error?.message || 'Erreur lors du chargement de la session.';
        state.error = detail;
      })
      .addCase(patchModelTests.fulfilled, (state, action) => {
        state.testResults = action.payload;
      })
      .addCase(
        fetchPage.fulfilled,
        (
          state,
          action: PayloadAction<
            | {
              rows?: any[] | null;
              total?: number | null;
              limit?: number | null;
              offset?: number | null;
              msgId?: string | null;
            }
            | null
            | undefined
          >
        ) => {
          // 1) Sécurise l’action.payload et ses champs
          const payload = action.payload ?? {};
          const {
            rows = null,
            total = 0,
            limit = 0,
            offset = 0,
            msgId = '',
          } = payload;

          // 2) Si pas de msgId, on ne fait rien
          if (!msgId) {
            return;
          }

          // 3) Récupère ou initialise le message
          state.queryComponentGraph = state.queryComponentGraph ?? {};
          const existing = state.queryComponentGraph[msgId];
          const msg = existing ?? {
            id: msgId,
            type: 'bot' as const,
            contents: {} as {
              sql?: string;
              real_res?: any[];
              meta?: DisplayTableMeta;
            },
            children: [] as unknown[],
          };

          // 4) Assure l’existence de contents
          msg.contents = msg.contents ?? {};

          // 5) Remplit real_res et meta
          //    On utilise rows ?? [] pour toujours avoir un tableau
          //    Pour la SQL, on récupère l’ancienne valeur stockée dans contents.meta.sql
          msg.contents.real_res = rows ?? [];
          msg.contents.meta = {
            total,
            limit,
            offset,
            sql: msg.contents.meta?.sql ?? '',
          } as DisplayTableMeta;

          // 6) Ré-assigne dans le state
          state.queryComponentGraph[msgId] = msg;
        }
      )


      // Optionnel : cas pending pour vider d’éventuelles erreurs
      .addCase(fetchPage.pending, (state, { meta }) => {
        const msgId = meta.arg.msgId;
        const msg = state.queryComponentGraph[msgId];
        if (msg) {
          delete msg.contents.error;
        }
      })

      // Optionnel : cas rejected pour stocker l’erreur
      .addCase(fetchPage.rejected, (state, { payload, meta }) => {
        const msgId = meta.arg.msgId;
        const msg = state.queryComponentGraph[msgId];
        if (msg) {
          msg.contents.error = typeof payload === 'string'
            ? payload
            : 'Une erreur est survenue.';
        }
      });
  },
});

export const { setError, resetMessages, setLoadingMessage, appendComponentToLastMessage,
  appendQueryComponentMessage, setTransformationName, setQueryComponentGraph,
  setValidateDataSuccess, setLoadingTestDataSuccess, resetContext, removeMessage, setSelectedChildIndex,
  setStep, setLoading, setQuery, setOptimizedQuery, setUserInput, addTextMessage, setSelectedDatabases,
  setTestResults, pushSqlHistory, setRestoredMessageId,
  appendStreamingReasoning, clearStreamingReasoning } = buildModelSlice.actions;

export default buildModelSlice.reducer;

