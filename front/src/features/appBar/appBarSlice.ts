import { PayloadAction, createSlice } from '@reduxjs/toolkit';
import { updateModel, deleteModel, fetchModels, createModel } from '../../api/models';
import { addProject, deleteProject, fetchProjects } from '../../api/projects';
import { handleRejectedCaseAppBar } from '../../utils/errorCase';
import { AppBarState, Model, Project } from '../../utils/types';


const initialState: AppBarState = {
    models: [],
    examples: [],
    projects: [],
    error: null,
    loadingAppBar: false,
    loadingSaveModel: false,
    openProjectDialog: false,
    drawerOpen: true,
    currentProjectId:''
};

export const buildAppBarSlice = createSlice({
    name: 'buildAppBar',

    initialState,
    reducers: {
        setModels: (state, action: PayloadAction<Model[]>) => {
            state.models = action.payload;
        },
        setProjects: (state, action: PayloadAction<Project[]>) => {
            const projects = action.payload;
            state.projects = projects;
            if (projects.length > 0) {
                state.currentProjectId = projects[0].project_id;
                state.currentProject = projects[0];
            }
        },
        setExamples: (state, action: PayloadAction<string[]>) => {
            state.examples = action.payload;
        },
        appendWelcomeExamples: (state, action: PayloadAction<string[]>) => {
          const newExamples=action.payload;
          if (state.examples && state.examples.length <=6) {
            state.examples = [...state.examples, ...newExamples];
          } else {
            state.examples = newExamples;
          }
        },
        setCurrentId: (state, action: PayloadAction<string | undefined>) => {
            state.currentModelId = action.payload;
            state.currentModel = state.models.find(model => model.session_id === action.payload);
        },
        setCurrentProjectId: (state, action: PayloadAction<string | undefined>) => {
            state.currentProjectId = action.payload || '';
            state.currentProject = state.projects.find(project => project.project_id === state.currentProjectId);
        },
        setCurrentModel: (state, action: PayloadAction<Model>) => {
            state.currentModel = action.payload;
        },
        updateModelName: (state, action: PayloadAction<{ session_id: string; name: string }>) => {
            state.models = state.models.map((model) =>
                model.session_id === action.payload.session_id
                    ? { ...model, name: action.payload.name }
                    : model
            );
        },
        setOpenProjectDialog: (state, action: PayloadAction<boolean>) => {
            state.openProjectDialog = action.payload;
        },
        toggleDrawer: (state) => {
            state.drawerOpen = !state.drawerOpen;
        },
    },
    extraReducers: (builder) => {
        builder
            .addCase(updateModel.pending, (state) => {
                state.loadingAppBar = true;
                state.error = null;
            })
            .addCase(updateModel.rejected, (state, action) => {
                state.loadingAppBar = false;
                state.error = action.payload as string;
            })
            .addCase(updateModel.fulfilled, (state, action) => {
                state.loadingAppBar = false;
                if (action.payload['existing']) {
                    state.models = state.models.map((model) => {
                        if (model.session_id === action.payload['session_id']) {
                            return {
                                ...model,
                                name: action.payload['name']
                            };
                        }
                        return model;
                    });
                } else {
                    state.error = `The model ${action.payload['id']} does not exist`;
                }
            })
            .addCase(createModel.pending, (state, action) => {
              state.loadingAppBar = true;
              state.error = null;

              // On récupère les données passées à l'action
              const { name, session_id } = action.meta.arg;

              // On ajoute directement le modèle « optimiste » en début de liste
              const newModel: Model = {
                session_id,
                name
              };

              state.models.unshift(newModel);
              state.currentModel = newModel;
              state.currentModelId = session_id;
              state.noReload = true;
            })
              .addCase(createModel.rejected, (state, action) => {
                state.loadingAppBar = false;
                state.error = action.payload as string;
        
                // On retire le modèle « optimiste » en se basant sur le session_id passé en paramètre
                const { session_id } = action.meta.arg;
                state.models = state.models.filter((model) => model.session_id !== session_id);
        
                // Si c'était le currentModel, on le reset
                if (state.currentModel?.session_id === session_id) {
                  state.currentModel = undefined;
                  state.currentModelId = undefined;
                }
              })
              .addCase(createModel.fulfilled, (state, action) => {
                state.loadingAppBar = false;
              })
            .addCase(fetchModels.pending, (state) => {
                state.loadingAppBar = true;
                state.error = null;
            })
            .addCase(fetchModels.fulfilled, (state, action) => {
                state.loadingAppBar = false;
                state.models = action.payload.map((f: any) => {
                    const fullName: string = f.test_name || f.name || '';
                    const lastSlash = fullName.lastIndexOf('/');
                    const folder = lastSlash >= 0 ? fullName.slice(0, lastSlash) : undefined;
                    const displayName = lastSlash >= 0 ? fullName.slice(lastSlash + 1) : fullName;
                    return {
                        session_id: f.session_id ?? f.name,
                        name: displayName,
                        folder,
                        updateDate: f.updated_at,
                        isTested: !!f.session_id,
                        modelName: f.model_name,
                        isStale: f.is_stale ?? false,
                        commitsSince: f.commits_since ?? 0,
                    };
                });
                state.currentModel = state.models.find(model => model.session_id === state.currentModelId);
                state.error = null;
            })
            .addCase(deleteModel.pending, (state) => {
                state.loadingAppBar = true;
            })
            .addCase(deleteModel.fulfilled, (state, action) => {
                state.loadingAppBar = false;
                state.success = 'model was deleted successfully';
                const session = action.meta.arg;
                state.models = state.models.filter(model => model.session_id !== session);
            })
            .addCase(fetchProjects.pending, (state) => {
                state.loadingAppBar = true;
                state.error = null;
            })
            .addCase(fetchProjects.fulfilled, (state, action) => {
                state.loadingAppBar = false;
                state.projects = action.payload;
                if (action.payload.length > 0 && !state.currentProjectId) {
                    state.currentProjectId = action.payload[0].project_id;
                    state.currentProject = action.payload[0];
                }
                state.error = null;
            })
            .addCase(addProject.pending, (state) => {
                state.loadingAppBar = true;
                state.error = null;
            })
            .addCase(addProject.fulfilled, (state, action) => {
                state.loadingAppBar = false;
                const project = JSON.parse(action.payload.json);

                // Vérifier si le projet existe déjà
                const existingProjectIndex = state.projects.findIndex(p => p.project_id === project.project_id);

                if (existingProjectIndex !== -1) {
                    // Mettre à jour le projet existant
                    state.projects[existingProjectIndex] = { ...state.projects[existingProjectIndex], ...project };
                } else {
                    // Ajouter le nouveau projet
                    state.projects = [...state.projects, project];
                }
                state.currentProjectId = project.project_id;
                state.currentProject = project;
            })
            .addCase(addProject.rejected, (state, action) => {
                state.loadingAppBar = false;
                state.error = action.payload as string;
            })

            .addCase(deleteProject.pending, (state) => {
                state.loadingAppBar = true;
            })
            .addCase(deleteProject.fulfilled, (state, action) => {
                state.loadingAppBar = false;
                state.success = 'Project was deleted successfully';
                const projectId = action.meta.arg;
                state.projects = state.projects.filter(project => project.project_id !== projectId);
                if (state.currentProjectId === projectId && state.projects.length>0) {
                    state.currentProjectId=state.projects[0].project_id
                }
            })
            .addCase(deleteProject.rejected, (state, action) => {
                    handleRejectedCaseAppBar(state, action, "Erreur lors de la suppression du projet");
            })
            .addCase(fetchProjects.rejected, (state, action) => {
                    handleRejectedCaseAppBar(state, action, "Erreur de chargement des projets");
            })
            .addCase(deleteModel.rejected, (state, action) => {
                    handleRejectedCaseAppBar(state, action, "Erreur lors de la suppression du modele");
            })
            .addCase(fetchModels.rejected, (state, action) => {
                    handleRejectedCaseAppBar(state, action, "Erreur lors du chargement des modeles");
            });
    },
});

export const { setModels, setProjects, setCurrentId, setCurrentProjectId, setOpenProjectDialog,
    setCurrentModel, setExamples, appendWelcomeExamples, toggleDrawer, updateModelName } = buildAppBarSlice.actions;

export default buildAppBarSlice.reducer;
