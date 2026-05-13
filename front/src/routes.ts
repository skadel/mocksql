export const ROUTES = {
  newTest: (model: string) => `/?model=${encodeURIComponent(model)}&forceNew=1`,
  testSession: (id: string) => `/models/${id}`,
  modelTests: (model: string) => `/sql/${encodeURIComponent(model)}`,
};
