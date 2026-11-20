import registry from "./configKeys.json" with { type: "json" };

const userKeys: string[] = registry.user;
const systemKeys: string[] = registry.system;
const startupOnlyKeys: string[] = registry.startupOnly;

export { startupOnlyKeys, systemKeys, userKeys };
