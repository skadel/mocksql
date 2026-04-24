
/**
 * Métadonnées renvoyées par l'API pour gérer la pagination serveur.
 */
export interface DisplayTableMeta {
  total: number; // nombre total de lignes
  limit: number; // nombre de lignes affichées
  offset: number; // offset courant (page = offset / limit)
  sql: string; // requête SQL d'origine
}

export const MsgType = {
  PROFILE_QUERY: 'profile_query',
} as const;

export interface ProfileRequest {
  message: string;
  profile_query: string;
  missing_columns: Array<{ table: string; used_columns: string[] }>;
  expected_joins?: Array<{ left_table: string; right_table: string }>;
  billing_tb?: number;
}

export interface MessageContents {
  text?: string;
  sql?: string;
  optimizedSql?: string;
  tables?: Record<string, Record<string, any>[]> | any[];
  res?: any[];
  real_res?: any[];
  meta?: DisplayTableMeta;
  error?: string;
  profileRequest?: ProfileRequest;
}

export interface Message {
  id: string;
  type: 'user' | 'bot';
  contents: MessageContents;
  parent?: string;
  children?: string[];
  sqlPrice?: number;
  sqlError?: string;
  contentType?: string | null;
  request?: string | null;
  testIndex?: number;
  context?: 'sql_update';
}

export type AnyRenderable = Message | MessageGroup;

export interface MessageGroup {
  type: 'group';
  parentId: string;
  branches: AnyRenderable[][];
}

export const isMessage = (x: AnyRenderable): x is Message =>
  !!x && (x as any).id !== undefined && (x as any).type !== 'group';

export const isMessageGroup = (x: AnyRenderable): x is MessageGroup =>
  (x as any)?.type === 'group';


export interface ColumnData {
  table_catalog: string;
  table_schema: string;
  table_name: string;
  field_path: string;
  data_type: string;
  primary_key?: boolean;
  equalityFilter?: boolean;
  description: string | null;
  is_categorical?: boolean;
}

export interface TableData {
  table: string;
  data: ColumnData[];
  primary_keys?: string[];
  equality_filters?: string[];
  hasChanged?: boolean
  description?: string;
}

export interface SqlHistoryEntry {
  id: string;
  sql: string;
  optimizedSql: string;
  parentMessageId: string; // ID of the last message when SQL was changed ('' for initial SQL)
}

export interface BuildModelState {
    activeStep: number;
    selectedDatabases: string[];
    error: string | null;
    executionError?: string;
    success?: boolean;
    step?: string;
    loading_message?: string;
    loading: boolean | null;
    testData?: TableData[];
    validateTestSuccess: boolean;
    validateTestLoading: boolean;
    queryComponentMessages: Message[];
    queryComponentGraph: Record<string, Message>;
    uniqueColumns: string[];
    query?: string;
    optimizedQuery?: string;
    testResults?: any[];
    name?: string;
    modelId?: string;
    lastUserInput: string;
    reasoning?: string;
    streamingReasoning?: string;
    sqlHistory: SqlHistoryEntry[];

    // New properties for storing table changes
    schemaDiff: {
        added: [string, object][];
        removed: [string, object][];
        modified: { [key: string]: any };
    };
    joinedSample: string;
    devOnlySample: string;
    prodOnlySample: string;
    selectedChildIndices: {
      [parentId: string]: number;
    };
    restoredMessageId?: string;
    lastError?: string;
}


export interface Model {
  id?: string;
  project_id?: string;
  query_id?: string;
  liked_query?: string;
  liked_analysis?: string;
  query_id_dev?: string;
  query_id_prod?: string;
  name?: string;
  session_id: string;
  user_sub?: string;
  creationDate?: string;
  updateDate?: string;
  isTested?: boolean;
  public?: boolean;
  refreshOption?: string;
  selected_columns?: Record<string, boolean>;
  tableType?: string;
  startDate?: string;
  selectedDayOfMonth?: number;
  selectedDayOfWeek?: string;
  selectedHour?: number;
  selectedMinute?: number;
  source?: boolean;
  source_project?: string;
  source_database?: string;
}

export interface Project {
    project_id: string;
    name: string;
    dialect: 'bigquery' | 'postgres';
    description: string;
    schema?: Record<string, any[]>;
    service_account_key?: string;
    auto_import?: boolean;
}


export interface MessageState {
    id: string;
    sender: 'user' | 'bot';
    type: string;
    message: string;
    content: string;
    creationDate?: string;
    updateDate?: string;
}

export interface AppBarState {
    models: Model[];
    projects: Project[];
    examples: string[];
    error: string | null;
    success?: string;
    loadingAppBar: boolean | null;
    loadingSaveModel: boolean | null;
    loadingSaveModelMessage?: string;
    saveSuccess?: boolean;
    currentModelId?: string;
    currentProjectId: string;
    currentProject?: Project;
    currentModel?: Model;
    openProjectDialog: boolean;
    noReload?: boolean;
    drawerOpen: boolean;
}

export interface ActionPayload {
    status?: number;
    detail?: string;
}


export interface CreateTableParams {
  modelId: string;
  currentProjectId: string;
  environment: string;
}

export interface CreateTableResponse {
  success: boolean;
  data: any;
}

export interface RejectValue {
  detail: string;
}

export interface ChatQueryParams {
  userInput: string;
  sessionId: string;
  project: string;
  dialect: string;
  query?: string;
  ChangedMessageId?: string;
  t: (key: string) => string;
  user?: string;
  parentMessageId?: string;
  userTables?: Record<string, Record<string, any>[]>;
  profileResult?: string;
  testIndex?: number;
  context?: 'sql_update';
}


export interface Example {
  // overview
  purpose?: string;
  id?: string;
  question_enriched?: string;
  main_question?: string;
  query_name?: string;

  // étapes CTE
  ctes: CteStep[];

  // méta
  final_sql?: string;
  sql?: string;
  storeId?: string;

  // statut
  isComplete: boolean;
}

export interface RawOutput {
  error: string;
  success: boolean;
  project: string;    // UUID du projet
  code: string;       // Le SQL complet
  dialect: string;
  examples: {
    main_question: string;
    query_name: string;
    purpose: string;
    question_enriched: string;
    requires_split: boolean;
    unsolvable: boolean;
    sub_questions: Array<{
      sub_question: string;
      sub_question_name: string;
      analysis: string;
      sub_query: string;
    }>;
    final_sql: string;
    store_id: string;
  };
}

// utils/types.ts  (ou dans le même fichier)
export interface CteStep {
  sub_question_name?: string;
  analysis?: string;
  build_tip?: string | null;
  sub_question?: string;
  sub_query?: string;
}

export interface ProjectState {
  activeStep: number;
  tables: TableData[];
  examples: Example[];
  projectName?: string;
  projectId?: string;
  error?: string;
  loading?: boolean;
  editedProjectId?: string;
  statistics?: string;
  train: Example;
  cost?: number;
  dialect: 'bigquery' | 'postgres';
  serviceAccountKey?: string;
}


export interface SubQuestion {
  sub_question:      string;
  sub_question_name: string;
  analysis:          string;
  sub_query:         string;
}

export interface DivideAndConquer {
  main_question:     string;
  query_name:        string;
  purpose:           string;
  question_enriched: string;
  requires_split:    boolean;
  sub_questions?:    SubQuestion[];
  unsolvable:        boolean;
  final_sql:         string;
}
