import { getRenderMessages, isStepMessage } from './getRenderMessages';
import { Message, RequestGroup, isRequestGroup } from '../utils/types';

const mk = (over: Partial<Message> & { id: string }): Message => ({
  type: 'bot',
  contents: {},
  ...over,
});

const select = (graph: Record<string, Message>) =>
  getRenderMessages({ buildModel: { queryComponentGraph: graph } } as any);

describe('getRenderMessages — regroupement par requête', () => {
  it('regroupe les messages bot d’une même requête en une bulle unique', () => {
    const graph: Record<string, Message> = {
      u1: mk({ id: 'u1', type: 'user', parent: undefined, contents: { text: 'modifie le test' } }),
      s1: mk({ id: 's1', parent: 'u1', request: 'R', contentType: 'generate_test_scenario', contents: { text: 'scénario' } }),
      ex1: mk({ id: 'ex1', parent: 's1', request: 'R', contentType: 'examples', contents: { tables: [{ t: 1 }] } }),
      ev1: mk({ id: 'ev1', parent: 'ex1', request: 'R', contentType: 'evaluation', contents: { text: '**Bon** — ok' } }),
      sg1: mk({ id: 'sg1', parent: 'ev1', request: 'R', contentType: 'suggestions', contents: { suggestions: ['a', 'b'] } }),
      fr1: mk({ id: 'fr1', parent: 'sg1', request: 'R', contentType: 'final_response', contents: { text: "J'ai modifié ton test." } }),
    };

    const out = select(graph);
    // [user, request_group]
    expect(out).toHaveLength(2);
    const rg = out[1];
    expect(isRequestGroup(rg)).toBe(true);
    const items = (rg as RequestGroup).items;
    expect((rg as RequestGroup).requestId).toBe('R');

    const steps = items.filter(isStepMessage);
    const visible = items.filter((m) => !isStepMessage(m));
    // scénario + examples + evaluation repliés ; final_response + suggestions visibles
    expect(steps.map((m) => m.contentType)).toEqual(
      expect.arrayContaining(['generate_test_scenario', 'examples', 'evaluation'])
    );
    expect(visible.map((m) => m.contentType)).toEqual(
      expect.arrayContaining(['suggestions', 'final_response'])
    );
  });

  it('ne regroupe pas un message bot isolé', () => {
    const graph: Record<string, Message> = {
      u1: mk({ id: 'u1', type: 'user', contents: { text: 'salut' } }),
      b1: mk({ id: 'b1', parent: 'u1', request: 'R', contentType: 'other', contents: { text: 'réponse' } }),
    };
    const out = select(graph);
    expect(out.some(isRequestGroup)).toBe(false);
  });
});
