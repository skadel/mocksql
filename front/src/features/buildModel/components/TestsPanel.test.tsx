import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';

// Composants lourds (exceljs, prismjs, barrel d'icônes) inutiles au chemin testé.
vi.mock('./DisplayTable', () => ({ default: () => null }));
vi.mock('../../../shared/SqlEditor', () => ({ default: () => null }));
vi.mock('../../../shared/ExcelDownloader', () => ({ default: () => null }));
vi.mock('../../../shared/ExcelUploader', () => ({ default: () => null }));

import buildModelReducer from '../buildModelSlice';
import appReducer from '../../appBar/appBarSlice';
import TestsPanel from './TestsPanel';

// Régression : pendant qu'un run est en cours (loading) et que le verdict LLM
// (test.evaluation) n'est pas encore arrivé, le badge de verdict ne doit PAS
// afficher le « Bon » optimiste dérivé du seul statut d'exécution (complete).
// L'UI doit montrer « Évaluation… » ; le badge n'apparaît qu'au verdict rendu
// ou hors run (données historiques rechargées).

type BuildModelState = ReturnType<typeof buildModelReducer>;

const makeStore = (overrides: Partial<BuildModelState>) =>
  configureStore({
    reducer: { buildModel: buildModelReducer, appBarModel: appReducer },
    preloadedState: {
      buildModel: { ...buildModelReducer(undefined, { type: '@@INIT' }), ...overrides },
      appBarModel: { ...appReducer(undefined, { type: '@@INIT' }), currentModelId: 'model-1' },
    },
  });

const completedTest = (extra: Record<string, any> = {}) => ({
  test_index: 0,
  status: 'complete',
  unit_test_description: 'Cas nominal — un client actif',
  tags: [],
  data: { clients: [{ id: 1 }] },
  results_json: JSON.stringify([{ id: 1 }]),
  ...extra,
});

const renderPanel = (buildModel: Record<string, any>) =>
  render(
    <Provider store={makeStore(buildModel)}>
      <TestsPanel
        onAddTest={vi.fn()}
        onSelectForModification={vi.fn()}
        selectedTestIndex={null}
      />
    </Provider>
  );

// Libellés UI en anglais (langue par défaut du produit) : badge « Good »,
// états « Evaluating… » / « Evaluation in progress… ».
describe("TestsPanel — badge de verdict pendant l'évaluation", () => {
  it("masque le badge « Good » tant que le verdict LLM n'est pas rendu (ligne compacte)", () => {
    renderPanel({ testResults: [completedTest()], loading: true });
    const row = screen.getByTestId('test-card-1');
    expect(within(row).queryByText('Good')).not.toBeInTheDocument();
    expect(within(row).getByText(/Evaluating/)).toBeInTheDocument();
  });

  it("masque le badge « Good » tant que le verdict LLM n'est pas rendu (carte dépliée)", () => {
    renderPanel({ testResults: [completedTest()], loading: true });
    fireEvent.click(screen.getByTestId('test-card-1'));
    const card = screen.getByTestId('test-card-1');
    expect(within(card).queryByText('Good')).not.toBeInTheDocument();
    expect(within(card).getByText('Evaluation in progress…')).toBeInTheDocument();
  });

  it('affiche le badge une fois le verdict LLM arrivé, même si le run continue', () => {
    renderPanel({
      testResults: [completedTest({ evaluation: '**Bon** — Données cohérentes.' })],
      loading: true,
    });
    const row = screen.getByTestId('test-card-1');
    expect(within(row).getByText('Good')).toBeInTheDocument();
    expect(within(row).queryByText(/Evaluating/)).not.toBeInTheDocument();
  });

  it("affiche le badge basé sur l'exécution hors run (modèle rechargé sans évaluation)", () => {
    renderPanel({ testResults: [completedTest()], loading: false });
    const row = screen.getByTestId('test-card-1');
    expect(within(row).getByText('Good')).toBeInTheDocument();
    expect(within(row).queryByText(/Evaluating/)).not.toBeInTheDocument();
  });

  it("ne masque pas le badge d'un autre test quand le run cible un test précis", () => {
    renderPanel({
      testResults: [completedTest(), completedTest({ test_index: 1 })],
      loading: true,
      loadingTestIndex: 1,
    });
    // Test 0 : pas concerné par le run → badge exec affiché.
    expect(within(screen.getByTestId('test-card-1')).getByText('Good')).toBeInTheDocument();
    // Test 1 : évaluation en cours → badge masqué.
    expect(within(screen.getByTestId('test-card-2')).queryByText('Good')).not.toBeInTheDocument();
  });
});
