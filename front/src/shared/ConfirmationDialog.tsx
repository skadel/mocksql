import { Dialog, DialogActions, DialogContent, DialogTitle, Typography } from '@mui/material';
import React from 'react';
import { DangerButton, NeutralGhostButton } from '../style/AppButtons';

interface ConfirmationDialogProps {
  open: boolean;
  onClose: () => void;
  onConfirm: () => void;
  title: string;
  message: string;
  confirmText?: string;
  cancelText?: string;
}

const ConfirmationDialog: React.FC<ConfirmationDialogProps> = ({
  open,
  onClose,
  onConfirm,
  title,
  message,
  confirmText = 'Confirm',
  cancelText = 'Cancel',
}) => {
  return (
    <Dialog open={open} onClose={onClose}>
      <DialogTitle>{title}</DialogTitle>
      <DialogContent>
        <Typography>{message}</Typography>
      </DialogContent>
      <DialogActions>
        <NeutralGhostButton onClick={onClose}>{cancelText}</NeutralGhostButton>
        <DangerButton onClick={onConfirm}>{confirmText}</DangerButton>
      </DialogActions>
    </Dialog>
  );
};

export default ConfirmationDialog;
