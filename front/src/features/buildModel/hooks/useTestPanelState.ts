import { useState } from 'react';

export type VerdictFilter = 'all' | 'good' | 'warn' | 'bad';

export interface TestPanelState {
  editingIndex: number | null;
  setEditingIndex: (idx: number | null) => void;
  editedDescriptions: Record<number, string>;
  setEditedDescriptions: React.Dispatch<React.SetStateAction<Record<number, string>>>;
  collapsed: Set<number>;
  setCollapsed: React.Dispatch<React.SetStateAction<Set<number>>>;
  filter: VerdictFilter;
  setFilter: (filter: VerdictFilter) => void;
  compact: boolean;
  setCompact: (compact: boolean) => void;
  openComments: Record<string, boolean>;
  setOpenComments: React.Dispatch<React.SetStateAction<Record<string, boolean>>>;
}

export function useTestPanelState(): TestPanelState {
  const [editingIndex, setEditingIndex] = useState<number | null>(null);
  const [editedDescriptions, setEditedDescriptions] = useState<Record<number, string>>({});
  const [collapsed, setCollapsed] = useState<Set<number>>(new Set());
  const [filter, setFilter] = useState<VerdictFilter>('all');
  const [compact, setCompact] = useState(false);
  const [openComments, setOpenComments] = useState<Record<string, boolean>>({});

  return {
    editingIndex, setEditingIndex,
    editedDescriptions, setEditedDescriptions,
    collapsed, setCollapsed,
    filter, setFilter,
    compact, setCompact,
    openComments, setOpenComments,
  };
}
