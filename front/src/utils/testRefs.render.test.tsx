import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import ReactMarkdown from 'react-markdown';
import { linkifyTestRefs } from './testRefs';

/**
 * Garde-fou : react-markdown v9 filtre par défaut les schémas d'URL « non sûrs » via
 * `defaultUrlTransform`. On vérifie qu'il LAISSE PASSER l'href ancre `#mocksql-test-N`
 * jusqu'au composant `a` — sinon le chip cliquable « test N » ne se rendrait jamais.
 */
describe('test-ref links survive react-markdown v9', () => {
  it('preserves the #mocksql-test-N anchor href down to the <a> renderer', () => {
    const md = linkifyTestRefs('Voir [[test:88eb]] qui échoue.', { '88eb': 2 });
    expect(md).toContain('[test 2](#mocksql-test-2)');

    let capturedHref: string | undefined;
    render(
      <ReactMarkdown
        components={{
          a: ({ href, children }: { href?: string; children?: React.ReactNode }) => {
            capturedHref = href;
            return <a data-testid="ref">{children}</a>;
          },
        }}
      >
        {md}
      </ReactMarkdown>
    );

    expect(capturedHref).toBe('#mocksql-test-2');
  });
});
