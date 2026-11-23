import lodash from "lodash";
import registry from "../configKeys.json" with { type: "json" };

const registeredNames = new Map<string, string>(
	[...registry.user, ...registry.system, ...registry.startupOnly].map(
		(name) => [lodash.camelCase(name), name],
	),
);

export const toKebabCasePreserveNumbers = (key = "") => {
	const registered = registeredNames.get(`${key}`);
	if (registered) {
		return registered;
	}
	return `${key}`
		.replace(/([A-Z])([A-Z][a-z])/g, "$1-$2")
		.replace(/([a-z0-9])([A-Z])/g, "$1-$2")
		.replace(/[_\s]+/g, "-")
		.toLowerCase()
		.replace(/^ed2-k(?=-|$)/, "ed2k");
};
