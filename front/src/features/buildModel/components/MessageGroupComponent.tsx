import React, { useEffect, useState } from 'react';
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import { setSelectedChildIndex } from '../buildModelSlice';
import { AnyRenderable, Message, MessageGroup } from '../../../utils/types';
import { styled } from '@mui/material/styles';
import IconButton from '@mui/material/IconButton';
import ArrowBackIosNewIcon from '@mui/icons-material/ArrowBackIosNew';
import ArrowForwardIosIcon from '@mui/icons-material/ArrowForwardIos';

const StyledNavContainer = styled('div')(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  gap: theme.spacing(0.5),
  marginBottom: '0',
}));

const PageNumber = styled('span')(({ theme }) => ({
  fontSize: '1rem',
  fontWeight: 500,
  color: theme.palette.text.primary,
  minWidth: 40,
  textAlign: 'center',
}));

const StyledIconButton = styled(IconButton)(({ theme }) => ({
  backgroundColor: theme.palette.background.paper,
  padding: theme.spacing(0.1),
  width: 24,
  height: 24,
  boxShadow: 'none',
  '&:hover': { backgroundColor: theme.palette.action.hover },
  '& .MuiSvgIcon-root': { fontSize: '16px' },
  '&.Mui-disabled': { opacity: 0.5 },
}));

interface MessageGroupComponentProps {
  group: MessageGroup;
  renderSingleMessage: (msg: Message, index: number) => JSX.Element;
}

const MessageGroupComponent: React.FC<MessageGroupComponentProps> = ({
  group,
  renderSingleMessage,
}) => {
  const dispatch = useAppDispatch();
  const { selectedChildIndices } = useAppSelector((state) => state.buildModel);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [selectedBranch, setSelectedBranch] = useState<AnyRenderable[] | undefined>();

  const immediateBranches = group.branches;
  const totalBranches = immediateBranches.length;

  useEffect(() => {
    const selected = selectedChildIndices?.[group.parentId] ?? (totalBranches - 1);
    setSelectedIndex(selected);
    setSelectedBranch(immediateBranches[selected]);
  }, [selectedChildIndices, group.parentId, immediateBranches, totalBranches]);

  const handleBranchSelect = (newIndex: number) => {
    if (newIndex >= 0 && newIndex < totalBranches) {
      dispatch(setSelectedChildIndex({ parentId: group.parentId, index: newIndex }));
      setSelectedIndex(newIndex);
      setSelectedBranch(immediateBranches[newIndex]);
    }
  };

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: '0px' }}>
        <StyledNavContainer>
          <StyledIconButton
            onClick={() => handleBranchSelect(selectedIndex - 1)}
            disabled={selectedIndex === 0}
            aria-label="Précédent"
          >
            <ArrowBackIosNewIcon fontSize="inherit" />
          </StyledIconButton>
          <PageNumber>{`${selectedIndex + 1}/${totalBranches}`}</PageNumber>
          <StyledIconButton
            onClick={() => handleBranchSelect(selectedIndex + 1)}
            disabled={selectedIndex === totalBranches - 1}
            aria-label="Suivant"
          >
            <ArrowForwardIosIcon fontSize="inherit" />
          </StyledIconButton>
        </StyledNavContainer>
      </div>

      {selectedBranch &&
        selectedBranch.map((item, index) => {
          if ((item as any)?.type === 'group') {
            return (
              <MessageGroupComponent
                key={`group-${(item as MessageGroup).parentId}`}
                group={item as MessageGroup}
                renderSingleMessage={renderSingleMessage}
              />
            );
          }

          const msg = item as Message;
          return (
            <React.Fragment key={msg.id}>
              {renderSingleMessage(msg, index)}
            </React.Fragment>
          );
        })}
    </div>
  );
};

export default React.memo(MessageGroupComponent);
