/**
 * Set version across all risuko npm packages
 *
 * Usage: node scripts/set-npm-version.mjs 0.2.0
 */
import { readFileSync, writeFileSync, readdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";

const version = process.argv[2];
if (!version) {
  console.error("Usage: node scripts/set-npm-version.mjs <version>");
  process.exit(1);
}

if (!/^\d+\.\d+\.\d+(-[\w.]+)?$/.test(version)) {
  console.error(`Invalid version: ${version}`);
  process.exit(1);
}

const root = resolve(import.meta.dirname, "..");

function updatePackageJson(filePath) {
  const raw = readFileSync(filePath, "utf-8");
  const pkg = JSON.parse(raw);
  const oldVersion = pkg.version;
  pkg.version = version;

  // Update dependency versions that point to risuko packages
  for (const depField of ["dependencies", "optionalDependencies"]) {
    if (pkg[depField]) {
      for (const dep of Object.keys(pkg[depField])) {
        if (dep.startsWith("@risuko/") || dep === "risuko-js" || dep === "risuko-cli") {
          pkg[depField][dep] = version;
        }
      }
    }
  }

  writeFileSync(filePath, JSON.stringify(pkg, null, 2) + "\n");
  console.log(`  ${pkg.name}: ${oldVersion} -> ${version}`);
}

function walkPackageJsons(dir) {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (entry === "package.json") {
      updatePackageJson(full);
    } else if (statSync(full).isDirectory() && entry !== "node_modules") {
      walkPackageJsons(full);
    }
  }
}

console.log(`Setting version to ${version}:\n`);

// Main packages
updatePackageJson(join(root, "packages/risuko-cli/package.json"));
updatePackageJson(join(root, "packages/risuko-js/package.json"));
updatePackageJson(join(root, "packages/risuko-app/package.json"));

// Platform sub-packages
walkPackageJsons(join(root, "packages/risuko-cli/npm"));
walkPackageJsons(join(root, "packages/risuko-js/npm"));

console.log("\nDone.");
