import React, { useState } from 'react';
import { Avatar, Box, Card, CardContent, Collapse, Typography } from '@mui/material';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import { useTranslation } from 'react-i18next';
import { Message, RequestGroup } from '../../../utils/types';
import { isStepMessage } from '../../../selectors/getRenderMessages';

const MONO = "'JetBrains Mono', ui-monospace, 'SF Mono', Menlo, Consolas, monospace";

interface RequestGroupBubbleProps {
  group: RequestGroup;
  renderBody: (msg: Message) => JSX.Element;
}

/**
 * Bulle unique regroupant tous les messages d'une requête.
 * - Réponse finale (texte) : visible.
 * - Étapes intermédiaires (scénario, données, exécution, évaluation, diagnostic) : repliées.
 * (Les suggestions ne sont plus dans le fil : panneau dédié, cf. TestsPanel.)
 */
const RequestGroupBubble: React.FC<RequestGroupBubbleProps> = ({ group, renderBody }) => {
  const { t } = useTranslation();
  const [stepsOpen, setStepsOpen] = useState(false);

  const steps = group.items.filter(isStepMessage);
  const finishItems = group.items.filter((m) => !isStepMessage(m));

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

          {/* Étapes intermédiaires repliées par défaut — toggle redesign Chat */}
          {steps.length > 0 && (
            <Box sx={{ mt: finishItems.length > 0 ? 1.25 : 0.5, borderTop: '1px solid #dae2e4', pt: 0.5 }}>
              <Box
                onClick={() => setStepsOpen((o) => !o)}
                data-testid="request-steps-toggle"
                sx={{
                  display: 'flex', alignItems: 'center', gap: 1, cursor: 'pointer',
                  py: 0.75, px: '2px', color: '#4f676b', '&:hover': { color: '#0f272a' },
                }}
              >
                <ChevronRightIcon sx={{ fontSize: 16, color: '#8da0a4', transform: stepsOpen ? 'rotate(90deg)' : 'none', transition: 'transform 0.17s' }} />
                <Typography component="span" sx={{ fontSize: 12.5, fontWeight: 600, color: 'inherit' }}>
                  {t('chatcol.steps')}
                </Typography>
                <Box sx={{ flex: 1 }} />
                <Box
                  sx={{
                    fontFamily: MONO, fontSize: 11, fontWeight: 700, minWidth: 20, height: 19,
                    px: '7px', borderRadius: '999px', bgcolor: '#f3f6f7', border: '1px solid #dae2e4',
                    color: '#4f676b', display: 'inline-grid', placeItems: 'center',
                  }}
                >
                  {steps.length}
                </Box>
              </Box>
              <Collapse in={stepsOpen}>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, pt: 0.5 }}>
                  {steps.map((m) => (
                    <React.Fragment key={m.id}>{renderBody(m)}</React.Fragment>
                  ))}
                </Box>
              </Collapse>
            </Box>
          )}
        </CardContent>
      </Card>
    </Box>
  );
};

export default React.memo(RequestGroupBubble);
