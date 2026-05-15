import reducer, {
  appendQueryComponentMessage,
  addTextMessage,
} from './buildModelSlice';
import type { BuildModelState } from '../../utils/types';
import type { Message } from '../../utils/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeMsg(overrides: Partial<Message> & { id: string }): Message {
  return {
    type: 'bot',
    contents: {},
    children: [],
    ...overrides,
  };
}

function makeUserMsg(id: string, parent?: string): Message {
  return { id, type: 'user', contents: { text: 'msg' }, parent, children: [] };
}

function makeSuggestionsMsg(id: string, parent: string, suggestions: string[]): Message {
  return makeMsg({
    id,
    parent,
    contentType: 'suggestions',
    contents: { suggestions },
  });
}

function makeExamplesMsg(id: string, parent: string, testIndex: number): Message {
  return makeMsg({
    id,
    parent,
    contentType: 'examples',
    contents: {
      tables: [{ test_index: testIndex, test_name: `test ${testIndex}` }],
    },
  });
}

function makeResultsMsg(id: string, parent: string, testIndex: number): Message {
  return makeMsg({
    id,
    parent,
    contentType: 'results',
    contents: {
      res: [{ test_index: testIndex, status: 'pass' }],
    },
  });
}

// Shorthand: run a sequence of actions through the reducer
function applyAll(initial: BuildModelState, messages: Message[]): BuildModelState {
  return messages.reduce(
    (state, msg) => reducer(state, appendQueryComponentMessage(msg)),
    initial,
  );
}

// ---------------------------------------------------------------------------
// Tests — suggestions message
// ---------------------------------------------------------------------------

describe('appendQueryComponentMessage — suggestions', () => {
  it('stores suggestions content and message id', () => {
    const initial = reducer(undefined, { type: '@@INIT' });
    const msg = makeSuggestionsMsg('sugg-1', 'user-1', ['Vérifie que A', 'Vérifie que B']);

    const state = reducer(initial, appendQueryComponentMessage(msg));

    expect(state.suggestions).toEqual(['Vérifie que A', 'Vérifie que B']);
    expect(state.lastSuggestionsMessageId).toBe('sugg-1');
  });

  it('updates lastSuggestionsMessageId on each new suggestions message', () => {
    const initial = reducer(undefined, { type: '@@INIT' });
    const sugg1 = makeSuggestionsMsg('sugg-1', 'user-1', ['A']);
    const sugg2 = makeSuggestionsMsg('sugg-2', 'user-2', ['B', 'C']);

    const state = applyAll(initial, [sugg1, sugg2]);

    expect(state.lastSuggestionsMessageId).toBe('sugg-2');
    expect(state.suggestions).toEqual(['B', 'C']);
  });
});

// ---------------------------------------------------------------------------
// Tests — génération 1 complète
// ---------------------------------------------------------------------------

describe('génération 1 — construction du graphe', () => {
  let stateAfterGen1: BuildModelState;

  beforeEach(() => {
    const initial = reducer(undefined, { type: '@@INIT' });
    const userMsg = makeUserMsg('user-1');
    const examples = makeExamplesMsg('examples-1', 'user-1', 0);
    const results = makeResultsMsg('results-1', 'user-1', 0);
    const sugg = makeSuggestionsMsg('sugg-1', 'results-1', ['Cas limite NULL', 'Plage vide']);

    let state = reducer(initial, addTextMessage(userMsg));
    stateAfterGen1 = applyAll(state, [examples, results, sugg]);
  });

  it('tous les messages de gen 1 sont dans queryComponentGraph', () => {
    const ids = Object.keys(stateAfterGen1.queryComponentGraph);
    expect(ids).toContain('user-1');
    expect(ids).toContain('examples-1');
    expect(ids).toContain('results-1');
    expect(ids).toContain('sugg-1');
  });

  it('les children de user-1 contiennent examples-1 et results-1', () => {
    const children = stateAfterGen1.queryComponentGraph['user-1'].children ?? [];
    expect(children).toContain('examples-1');
    expect(children).toContain('results-1');
  });

  it('testResults contient le test de gen 1 avec status pending puis pass', () => {
    expect(stateAfterGen1.testResults).toHaveLength(1);
    expect(stateAfterGen1.testResults![0].test_index).toBe(0);
    expect(stateAfterGen1.testResults![0].status).toBe('pass');
  });

  it('lastSuggestionsMessageId pointe sur sugg-1', () => {
    expect(stateAfterGen1.lastSuggestionsMessageId).toBe('sugg-1');
  });
});

// ---------------------------------------------------------------------------
// Tests — génération 2 : chaînage sur suggestions de gen 1
// ---------------------------------------------------------------------------

describe('génération 2 — chaînage sur lastSuggestionsMessageId', () => {
  let stateAfterGen2: BuildModelState;

  beforeEach(() => {
    const initial = reducer(undefined, { type: '@@INIT' });

    // Gen 1
    const user1 = makeUserMsg('user-1');
    const examples1 = makeExamplesMsg('examples-1', 'user-1', 0);
    const results1 = makeResultsMsg('results-1', 'user-1', 0);
    const sugg1 = makeSuggestionsMsg('sugg-1', 'results-1', ['Cas limite NULL', 'Plage vide']);

    let state = reducer(initial, addTextMessage(user1));
    state = applyAll(state, [examples1, results1, sugg1]);

    // Gen 2 : user message parenté à sugg-1 (lastSuggestionsMessageId)
    const user2 = makeUserMsg('user-2', state.lastSuggestionsMessageId);
    const examples2 = makeExamplesMsg('examples-2', 'user-2', 1);
    const results2 = makeResultsMsg('results-2', 'user-2', 1);
    const sugg2 = makeSuggestionsMsg('sugg-2', 'results-2', ['Ex æquo', 'Format de sortie']);

    state = reducer(state, addTextMessage(user2));
    stateAfterGen2 = applyAll(state, [examples2, results2, sugg2]);
  });

  it('user-2 a bien sugg-1 comme parent', () => {
    expect(stateAfterGen2.queryComponentGraph['user-2'].parent).toBe('sugg-1');
  });

  it('sugg-1 a user-2 dans ses children', () => {
    const children = stateAfterGen2.queryComponentGraph['sugg-1'].children ?? [];
    expect(children).toContain('user-2');
  });

  it('les messages de gen 1 sont toujours présents dans queryComponentGraph', () => {
    const ids = Object.keys(stateAfterGen2.queryComponentGraph);
    expect(ids).toContain('user-1');
    expect(ids).toContain('examples-1');
    expect(ids).toContain('results-1');
    expect(ids).toContain('sugg-1');
  });

  it('les messages de gen 2 sont ajoutés sans écraser gen 1', () => {
    const ids = Object.keys(stateAfterGen2.queryComponentGraph);
    expect(ids).toContain('user-2');
    expect(ids).toContain('examples-2');
    expect(ids).toContain('results-2');
    expect(ids).toContain('sugg-2');
    expect(ids).toHaveLength(8); // 4 de gen1 + 4 de gen2
  });

  it('testResults fusionne les tests de gen 1 et gen 2 par test_index', () => {
    expect(stateAfterGen2.testResults).toHaveLength(2);
    const indices = stateAfterGen2.testResults!.map((t: any) => t.test_index);
    expect(indices).toContain(0);
    expect(indices).toContain(1);
  });

  it('lastSuggestionsMessageId pointe sur sugg-2 après gen 2', () => {
    expect(stateAfterGen2.lastSuggestionsMessageId).toBe('sugg-2');
  });

  it('les suggestions sont mises à jour avec celles de gen 2', () => {
    expect(stateAfterGen2.suggestions).toEqual(['Ex æquo', 'Format de sortie']);
  });
});

// ---------------------------------------------------------------------------
// Tests — génération 3 : chaîne à 3 niveaux
// ---------------------------------------------------------------------------

describe('génération 3 — chaîne sugg1 → user2 → sugg2 → user3', () => {
  it('user-3 a sugg-2 comme parent et sugg-2 a user-3 dans ses children', () => {
    const initial = reducer(undefined, { type: '@@INIT' });

    // Gen 1
    let state = reducer(initial, addTextMessage(makeUserMsg('user-1')));
    state = applyAll(state, [
      makeExamplesMsg('examples-1', 'user-1', 0),
      makeResultsMsg('results-1', 'user-1', 0),
      makeSuggestionsMsg('sugg-1', 'results-1', ['A']),
    ]);

    // Gen 2
    state = reducer(state, addTextMessage(makeUserMsg('user-2', state.lastSuggestionsMessageId)));
    state = applyAll(state, [
      makeExamplesMsg('examples-2', 'user-2', 1),
      makeResultsMsg('results-2', 'user-2', 1),
      makeSuggestionsMsg('sugg-2', 'results-2', ['B']),
    ]);

    // Gen 3
    state = reducer(state, addTextMessage(makeUserMsg('user-3', state.lastSuggestionsMessageId)));
    state = applyAll(state, [
      makeExamplesMsg('examples-3', 'user-3', 2),
      makeResultsMsg('results-3', 'user-3', 2),
      makeSuggestionsMsg('sugg-3', 'results-3', ['C']),
    ]);

    expect(state.queryComponentGraph['user-3'].parent).toBe('sugg-2');
    expect(state.queryComponentGraph['sugg-2'].children).toContain('user-3');
    expect(state.testResults).toHaveLength(3);
    expect(state.lastSuggestionsMessageId).toBe('sugg-3');
  });
});
