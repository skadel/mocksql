import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { chatQuery, fetchPage } from '../../api/query';
import { fetchUniqueColumns, getTableChanges } from '../../api/table';
import { getMessages, patchModelTests } from '../../api/messages';
import { handleRejectedCase } from '../../utils/errorCase';
import { BuildModelState, DisplayTableMeta, Message, MessageContents, SqlHistoryEntry } from '../../utils/types';
import { formatMessage } from '../../utils/messages';

const initialState: BuildModelState = {
  activeStep: 0,
  selectedDatabases: [],
  error: null,
  loading: null,
  name: '',
  validateTestSuccess: false,
  validateTestLoading: false,
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
  workspaceMode: false,
  suggestions: [],
  suggestionRationales: {},
  retryBadDataTestIndex: undefined,
  testsTarget: undefined,
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
      const silent = (msg as any).silent === true;

      // Suggestions = panneau dédié, jamais dans le fil : on alimente state.suggestions
      // (canal live SSE) et on n'insère pas le message dans queryComponentGraph.
      if (msg.contentType === 'suggestions') {
        if (Array.isArray(msg.contents.suggestions)) state.suggestions = msg.contents.suggestions;
        state.suggestionRationales = msg.contents.rationales ?? {};
        return;
      }

      if (msg.contentType === 'retry_prompt') {
        state.retryBadDataTestIndex = msg.testIndex ?? null;
      }

      if (!silent) {
        state.queryComponentGraph[msg.id] = msg;
        if (msg.parent && state.queryComponentGraph[msg.parent]) {
          const parentMessage = state.queryComponentGraph[msg.parent];
          parentMessage.children = parentMessage.children || [];
          parentMessage.children.push(msg.id);
        }
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
        const newTests = (msg.contents.tables as any[]).map((t: any) => ({ ...t, status: 'pending', threadParentId: msg.parent }));
        const base = msg.context === 'sql_update'
          ? (state.testResults || []).map((t: any) => ({ ...t, status: 'pending' }))
          : [...(state.testResults || [])];
        newTests.forEach((newT: any) => {
          const idx = base.findIndex((r: any) => r.test_index === newT.test_index);
          if (idx >= 0) base[idx] = { ...newT, threadParentId: newT.threadParentId ?? base[idx].threadParentId };
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

      // Remove a test case deleted by the conversational agent
      if (msg.contentType === 'delete_test') {
        const testIndex = (msg.contents as any).testIndex ?? msg.testIndex;
        state.testResults = (state.testResults || []).filter(
          (t: any) => String(t.test_index) !== String(testIndex)
        );
      }

      // Update name/description of a test renamed by the conversational agent
      if (msg.contentType === 'update_test') {
        const testIndex = (msg.contents as any).testIndex ?? msg.testIndex;
        const newName = (msg.contents as any).newName;
        const newDescription = (msg.contents as any).newDescription;
        state.testResults = (state.testResults || []).map((t: any) => {
          if (String(t.test_index) !== String(testIndex)) return t;
          return {
            ...t,
            ...(newName ? { test_name: newName } : {}),
            ...(newDescription ? { unit_test_description: newDescription } : {}),
            // Validation acceptée (accept_validation) : la désync est résolue → on retire le
            // marqueur (needs_validation OU bad_description) pour faire disparaître le prompt
            // (le verdict Bon arrive via le message EVALUATION qui suit).
            ...(t.reason_type === 'needs_validation' || t.reason_type === 'bad_description'
              ? { reason_type: null, expected_row_count: undefined, corrected_description: undefined, corrected_name: undefined }
              : {}),
          };
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
    resetContext: () => {
      return initialState;
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
    resetMessages(state) {
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
    // Consommation optimiste d'une suggestion : on la retire du panneau dès le clic
    // (le backend la retire aussi du modèle, cf. history_saver/suggestion_intent).
    dismissSuggestion(state, action: PayloadAction<string>) {
      state.suggestions = state.suggestions.filter((s) => s !== action.payload);
      const { [action.payload]: _removed, ...rest } = state.suggestionRationales ?? {};
      state.suggestionRationales = rest;
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
    setWorkspaceMode(state, action: PayloadAction<boolean>) {
      state.workspaceMode = action.payload;
    },
    patchMessageContents(state, action: PayloadAction<{ id: string; patch: Partial<MessageContents> }>) {
      const msg = state.queryComponentGraph[action.payload.id];
      if (!msg) return;
      Object.assign(msg.contents, action.payload.patch);
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
      state.testResults = [];
      state.suggestions = [];
      state.suggestionRationales = {};
    })
      .addCase(getMessages.fulfilled, (state, action: PayloadAction<{ messages: any[]; sql: string | null; optimized_sql: string | null; test_results: any[]; suggestions?: string[]; suggestion_rationales?: Record<string, string>; restored_message_id?: string | null; tests_target?: number | null; last_error?: string | null; sql_history?: SqlHistoryEntry[] }>) => {
        const { messages, sql, optimized_sql, test_results, suggestions, suggestion_rationales, restored_message_id, tests_target, last_error, sql_history } = action.payload;
        state.error = '';
        state.loading = false;
        if (sql) state.query = sql;
        if (optimized_sql) state.optimizedQuery = optimized_sql;
        if (test_results?.length) state.testResults = test_results;
        // Objectif du batch : sert à détecter une boucle multi-tests interrompue (reprise).
        state.testsTarget = tests_target ?? undefined;
        // Suggestions = état du modèle (panneau dédié), chargé comme test_results.
        if (suggestions?.length) state.suggestions = suggestions;
        state.suggestionRationales = suggestion_rationales ?? {};
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

        // Fold evaluation into its target message so history renders as one bubble.
        // Target: nearest generate_test_scenario or examples ancestor in the parent chain.
        // (Suggestions ne sont plus dans l'historique : elles vivent dans le panneau dédié,
        // chargées depuis le champ modèle `suggestions`.)
        {
          const toDelete = new Set<string>();
          messages.forEach((raw: any) => {
            const msg = state.queryComponentGraph[raw.id];
            if (!msg) return;
            if (msg.contentType !== 'evaluation') return;

            let targetId: string | null = null;
            let curId = msg.parent;
            const visited = new Set<string>();
            while (curId && !visited.has(curId)) {
              visited.add(curId);
              const cur = state.queryComponentGraph[curId];
              if (!cur) break;
              if (cur.contentType === 'generate_test_scenario') { targetId = curId; break; }
              if (cur.contentType === 'examples') { targetId = curId; break; }
              if (cur.type === 'user') break;
              curId = cur.parent ?? undefined;
            }

            if (!targetId) return;
            const target = state.queryComponentGraph[targetId];

            // Only embed evaluation text for conversational flow (generate_test_scenario target).
            // For initial generation the verdict is already in testResults / TestsPanel.
            if (target.contentType === 'generate_test_scenario') {
              target.contents.evaluationText = msg.contents.text;
            }

            const parentMsg = msg.parent ? state.queryComponentGraph[msg.parent] : null;
            if (parentMsg?.children) {
              parentMsg.children = parentMsg.children.filter(id => id !== msg.id);
            }
            toDelete.add(msg.id);
          });
          toDelete.forEach(id => delete state.queryComponentGraph[id]);
        }

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

        // (Les suggestions sont chargées depuis le champ modèle `suggestions` plus haut,
        // plus besoin de les reconstruire depuis l'historique de conversation.)

        // 4. Déterminer le dernier message et mettre à jour selectedChildIndices
        const latestMessage = messages[messages.length - 1];
        if (latestMessage) {
          setDefaultBranchSelection(state, latestMessage.id);
        }

        state.selectedDatabases = [];
      })
      .addCase(chatQuery.pending, (state, action) => {
        state.loading = true;
        state.streamingReasoning = undefined;
        state.lastReasoning = undefined;
        state.retryBadDataTestIndex = undefined;
        const { testIndex, assertionOnly, validateIntent } = action.meta.arg;
        state.loadingTestIndex = testIndex;
        // « Je valide l'état actuel » : ce n'est PAS une régénération. Les données, résultats
        // et assertions stockés restent valides (input + SQL inchangés) — accept_validation ne
        // ré-exécute rien. Passer le test en `pending` afficherait un loader « Exécution… » qui
        // ne se résoudrait jamais (aucun message RESULTS ne suit). On garde donc l'état affiché ;
        // seuls la description (UPDATE_TEST) et le verdict (EVALUATION) seront mis à jour.
        if (testIndex !== undefined && state.testResults?.length && !validateIntent) {
          state.testResults = state.testResults.map((t: any) => {
            if (t.test_index !== testIndex) return t;
            if (assertionOnly) {
              // Assertion-only: garde les données, efface juste l'évaluation
              return { ...t, evaluation: undefined };
            } else {
              // Régénération complète : pending immédiat sans attendre les SSE
              return { ...t, status: 'pending', evaluation: undefined };
            }
          });
        }
      })
      .addCase(chatQuery.fulfilled, (state) => {
        state.loading = false;
        state.loading_message = undefined;
        state.lastReasoning = state.streamingReasoning || undefined;
        state.streamingReasoning = undefined;
        state.loadingTestIndex = undefined;
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
        state.loadingTestIndex = undefined;
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
  setLoading, setQuery, setOptimizedQuery, setUserInput, addTextMessage, setSelectedDatabases,
  setTestResults, dismissSuggestion, pushSqlHistory, setRestoredMessageId,
  appendStreamingReasoning, clearStreamingReasoning, setWorkspaceMode, patchMessageContents } = buildModelSlice.actions;

export default buildModelSlice.reducer;

