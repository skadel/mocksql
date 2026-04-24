import { configureStore } from '@reduxjs/toolkit';
import buildModelReducer from '../features/buildModel/buildModelSlice';
import appReducer from '../features/appBar/appBarSlice';


export const store = configureStore({
  reducer: {
    buildModel: buildModelReducer,
    appBarModel: appReducer,
  },
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;
