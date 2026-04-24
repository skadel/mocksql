export const validateProjectName = (name: string, setProjectNameError: (arg0: string) => void) => {
  if (!name.trim()) {
    setProjectNameError('Le nom du projet est obligatoire. Veuillez entrer un nom.');
    return false;
  }
  const regex = /^[a-zA-Z0-9_]+$/;
  if (!regex.test(name)) {
    setProjectNameError(
      'Le nom du projet est invalide. Il doit uniquement contenir des lettres, des chiffres ou des underscores (_).'
    );
    return false;
  }
  setProjectNameError('');
  return true;
};
