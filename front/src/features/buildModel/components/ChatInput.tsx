import React, { useCallback, useEffect, useState } from 'react';
import DroppableTextField from '../../../shared/DroppableTextField';

type ChatInputProps = {
  /** id du modèle courant — sert de clé de brouillon (draft) */
  modelId?: string;
  /** envoi du message ; retourne true si consommé (→ on vide le champ) */
  onSend: (text: string) => Promise<boolean> | boolean;
  onStopStream: () => void;
  placeholder?: string;
  inputRef?: React.Ref<HTMLInputElement>;
};

const draftKeyFor = (modelId?: string) => `draft:${modelId || 'new'}`;

/**
 * Champ de saisie du chat **isolé** : le texte tapé vit dans l'état local de ce
 * composant, donc une frappe ne re-render QUE ce composant — pas
 * `QueryChatComponent` (1700+ lignes) ni le fil de messages. C'est la correction
 * de la latence de frappe : avant, `userInput` était remonté tout en haut et
 * chaque caractère re-rendait tout l'arbre + écrivait en localStorage de façon
 * synchrone. Ici le brouillon est persisté de façon débouncée.
 */
const ChatInput: React.FC<ChatInputProps> = ({
  modelId,
  onSend,
  onStopStream,
  placeholder,
  inputRef,
}) => {
  const [text, setText] = useState('');

  // Restaure le brouillon quand on change de modèle
  useEffect(() => {
    setText(localStorage.getItem(draftKeyFor(modelId)) ?? '');
  }, [modelId]);

  // Persiste le brouillon de façon débouncée (pas à chaque frappe)
  useEffect(() => {
    const id = setTimeout(() => {
      localStorage.setItem(draftKeyFor(modelId), text);
    }, 300);
    return () => clearTimeout(id);
  }, [text, modelId]);

  const handleSend = useCallback(async () => {
    const ok = await onSend(text);
    if (ok) {
      setText('');
      localStorage.removeItem(draftKeyFor(modelId));
    }
  }, [onSend, text, modelId]);

  return (
    <DroppableTextField
      userInput={text}
      setUserInput={setText}
      sendMessage={handleSend}
      stopStream={onStopStream}
      inputRef={inputRef}
      placeholder={placeholder}
    />
  );
};

export default React.memo(ChatInput);
