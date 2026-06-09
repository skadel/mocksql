import React, { useState } from 'react';
import { Avatar, Box, Card, CardContent, Chip, Collapse, Typography } from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { Message, RequestGroup } from '../../../utils/types';
import { isStepMessage } from '../../../selectors/getRenderMessages';

interface RequestGroupBubbleProps {
  group: RequestGroup;
  renderBody: (msg: Message) => JSX.Element;
}

const isSuggestionMsg = (m: Message): boolean =>
  m.contentType === 'suggestions' ||
  (Array.isArray(m.contents?.suggestions) && (m.contents!.suggestions as any[]).length > 0);

/**
 * Bulle unique regroupant tous les messages d'une requête.
 * - Réponse finale (texte) + suggestions : visibles.
 * - Étapes intermédiaires (scénario, données, exécution, évaluation, diagnostic) : repliées.
 */
const RequestGroupBubble: React.FC<RequestGroupBubbleProps> = ({ group, renderBody }) => {
  const [stepsOpen, setStepsOpen] = useState(false);

  const steps = group.items.filter(isStepMessage);
  const visible = group.items.filter((m) => !isStepMessage(m));
  const suggestionItems = visible.filter(isSuggestionMsg);
  const finishItems = visible.filter((m) => !isSuggestionMsg(m));

  return (
    <Box sx={{ display: 'flex', justifyContent: 'flex-start', my: 0.5 }}>
      <Card
        variant="outlined"
        sx={{ backgroundColor: 'white', borderRadius: '12px', boxShadow: 1, width: '100%', overflow: 'visible' }}
      >
        {/* Header compact */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, px: 1.5, pt: 0.75, pb: 0 }}>
          <Avatar src="/static/logo192.png" sx={{ width: 22, height: 22 }} />
          <Typography variant="caption" sx={{ fontWeight: 700, color: '#555' }}>
            MockSQL
          </Typography>
        </Box>

        <CardContent sx={{ px: 1.5, py: 0.75, '&:last-child': { pb: 0.75 } }}>
          {/* Réponse finale (visible) */}
          {finishItems.map((m) => (
            <React.Fragment key={m.id}>{renderBody(m)}</React.Fragment>
          ))}

          {/* Étapes intermédiaires repliées par défaut */}
          {steps.length > 0 && (
            <Box sx={{ mt: finishItems.length > 0 ? 1 : 0.5 }}>
              <Box
                onClick={() => setStepsOpen((o) => !o)}
                data-testid="request-steps-toggle"
                sx={{
                  display: 'inline-flex', alignItems: 'center', gap: 0.5, cursor: 'pointer',
                  color: '#888', fontSize: 12,
                  '&:hover': { color: '#555' },
                }}
              >
                <ExpandMoreIcon sx={{ fontSize: 14, transform: stepsOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }} />
                Étapes
                <Chip
                  label={steps.length}
                  size="small"
                  sx={{ ml: 0.25, height: 16, fontSize: 10, fontWeight: 700, bgcolor: '#eef4f4', color: '#6b8287' }}
                />
              </Box>
              <Collapse in={stepsOpen}>
                <Box sx={{ mt: 0.5, pl: 1, borderLeft: '2px solid #e8f0f0' }}>
                  {steps.map((m) => (
                    <React.Fragment key={m.id}>{renderBody(m)}</React.Fragment>
                  ))}
                </Box>
              </Collapse>
            </Box>
          )}

          {/* Suggestions (visibles, sous la réponse) */}
          {suggestionItems.map((m) => (
            <React.Fragment key={m.id}>{renderBody(m)}</React.Fragment>
          ))}
        </CardContent>
      </Card>
    </Box>
  );
};

export default React.memo(RequestGroupBubble);
