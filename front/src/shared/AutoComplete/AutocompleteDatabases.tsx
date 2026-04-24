import React from 'react';
import { fetchListTablesAndDatasets } from '../../api/table';
import { useAppDispatch } from '../../app/hooks';
import AutocompleteInput from './AutocompleteInput';

interface AutocompleteDatabasesProps {
  labelName?: string;
  selectedDatabase: string;
  setSelectedDatabase: (database: string) => void;
}

const AutocompleteDatabases: React.FC<AutocompleteDatabasesProps> = ({
  labelName,
  selectedDatabase,
  setSelectedDatabase,
}) => {
  const dispatch = useAppDispatch();

  const fetchSuggestions = async (inputValue: string) => {
    try {
      const response = await dispatch(
        fetchListTablesAndDatasets({ inputValue })
      ).unwrap();
      return response || [];
    } catch (error) {
      console.error('Erreur lors de la récupération des suggestions :', error);
      return [];
    }
  };

  const handleSelectDatabase = (database: string | null) => {
    if (database) {
      setSelectedDatabase(database);
    }
  };

  return (
    <div>
      <AutocompleteInput
        onSelectSuggestion={handleSelectDatabase}
        fetchSuggestions={fetchSuggestions}
        labelName={labelName || 'Nom de la base de données ou de la table'}
        resetInputValueOnSelect={false}
        value={selectedDatabase}
        setValue={setSelectedDatabase}
      />
    </div>
  );
};

export default AutocompleteDatabases;
