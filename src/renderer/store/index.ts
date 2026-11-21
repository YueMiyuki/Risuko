import { createPinia, setActivePinia } from "pinia";

const pinia = createPinia();
setActivePinia(pinia);

export default pinia;

export * from "./app";
export * from "./auth";
export * from "./health";
export * from "./preference";
export * from "./sync";
export * from "./task";
