import React from 'react';
import { Box, Typography } from '@mui/material';
import { useNavigate } from 'react-router-dom';
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import { resetContext } from '../../buildModel/buildModelSlice';
import { setCurrentId } from '../appBarSlice';

const TEAL = '#2BB0A8';
const MUTED = '#6b8287';

function relativeDate(iso?: string): string {
  if (!iso) return '';
  const diff = Date.now() - new Date(iso).getTime();
  const m = Math.floor(diff / 60000);
  if (m < 1)   return "à l'instant";
  if (m < 60)  return `il y a ${m} min`;
  const h = Math.floor(m / 60);
  if (h < 24)  return `il y a ${h} h`;
  const d = Math.floor(h / 24);
  return `il y a ${d} j`;
}

interface Props {
  search: string;
}

const SqlFileList: React.FC<Props> = ({ search }) => {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const models   = useAppSelector(s => s.appBarModel.models);
  const currentModelId = useAppSelector(s => s.appBarModel.currentModelId);

  const q = search.toLowerCase().trim();

  const filtered = models.filter(m => {
    const label = (m.name ?? m.session_id ?? '').toLowerCase();
    return !q || label.includes(q);
  });

  if (filtered.length === 0) {
    return (
      <Box sx={{ p: '24px 16px', textAlign: 'center' }}>
        <Typography sx={{ fontSize: 12.5, color: MUTED }}>
          {q ? 'Aucun résultat' : "Aucun modèle pour l'instant"}
        </Typography>
      </Box>
    );
  }

  return (
    <>
      {filtered.map(model => {
        const isTested = model.isTested ?? true;
        // For tested models session_id is the real UUID; for untested it equals model name.
        const isActive = isTested && model.session_id === currentModelId;
        const displayName = model.name || model.session_id;

        const handleClick = () => {
          dispatch(resetContext());
          if (isTested) {
            dispatch(setCurrentId(model.session_id));
            navigate(`/models/${model.session_id}`);
          } else {
            // Untested: open the generator pre-filled with this SQL file name
            dispatch(setCurrentId(''));
            navigate(`/?model=${encodeURIComponent(model.session_id)}`);
          }
        };

        return (
          <Box
            key={model.session_id}
            onClick={handleClick}
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: '10px',
              px: '12px',
              py: '8px',
              mx: '6px',
              borderRadius: '8px',
              cursor: 'pointer',
              bgcolor: isActive ? '#e6f4f3' : 'transparent',
              '&:hover': { bgcolor: isActive ? '#e6f4f3' : '#edf0f1' },
              transition: 'background .12s',
            }}
          >
            <Box
              sx={{
                width: 8, height: 8, borderRadius: '50%',
                bgcolor: isActive ? TEAL : isTested ? '#23a26d' : '#c9d3d6',
                flexShrink: 0, mt: '1px',
              }}
            />

            <Box sx={{ flex: 1, minWidth: 0 }}>
              <Typography
                noWrap
                sx={{
                  fontSize: 13, fontWeight: isActive ? 600 : 500,
                  color: isActive ? TEAL : '#0f272a', lineHeight: 1.3,
                }}
              >
                {displayName}
              </Typography>
              <Typography sx={{ fontSize: 11, color: '#a0adb0', whiteSpace: 'nowrap', mt: '1px' }}>
                {isTested ? relativeDate(model.updateDate) : 'Non testé'}
              </Typography>
            </Box>
          </Box>
        );
      })}
    </>
  );
};

export default SqlFileList;
