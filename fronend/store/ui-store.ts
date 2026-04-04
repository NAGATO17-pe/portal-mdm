import { createContext } from "react";

export type UiStore = {
  sidebarOpen: boolean;
  setSidebarOpen: (value: boolean) => void;
};

export const UiStoreContext = createContext<UiStore | null>(null);
