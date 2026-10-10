#!/usr/bin/env node
import {
  existsSync,
  mkdtempSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const root = resolve(import.meta.dirname, "..");

const args = process.argv.slice(2);
const versionArg = args.find((arg) => !arg.startsWith("--"));
const version = versionArg || "0.0.0-bootstrap.0";
const dryRun = args.includes("--dry-run");

const tagIndex = args.indexOf("--tag");
const tag = tagIndex !== -1 && args[tagIndex + 1] ? args[tagIndex + 1] : "bootstrap";

if (!/^\d+\.\d+\.\d+(-[\w.]+)?$/.test(version)) {
  console.error(`Invalid version: ${version}`);
  process.exit(1);
}

if (!tag || tag.startsWith("-") || /\s/.test(tag)) {
  console.error(`Invalid npm tag: ${tag}`);
  process.exit(1);
}

function run(command, commandArgs = [], cwd = root, allowFailure = false) {
  const result = spawnSync(command, commandArgs, {
    cwd,
    stdio: ["ignore", "pipe", "pipe"],
    encoding: "utf8",
    shell: false,
  });
  if (result.status === 0) {
    return result.stdout.trim();
  }
  if (allowFailure) {
    return "";
  }
  throw new Error(
    `Command failed: ${[command, ...commandArgs].join(" ")}\n${result.stderr}`,
  );
}

function listPackageDirs(baseDir) {
  const absBase = join(root, baseDir);
  return readdirSync(absBase)
    .map((entry) => join(absBase, entry))
    .filter((fullPath) => existsSync(join(fullPath, "package.json")));
}

function readPkg(pkgDir) {
  return JSON.parse(readFileSync(join(pkgDir, "package.json"), "utf8"));
}

function writePkg(pkgDir, pkg) {
  writeFileSync(join(pkgDir, "package.json"), `${JSON.stringify(pkg, null, 2)}\n`);
}

function updateInternalDeps(pkg, nextVersion) {
  for (const field of ["dependencies", "optionalDependencies"]) {
    if (!pkg[field]) continue;
    for (const dep of Object.keys(pkg[field])) {
      if (dep.startsWith("@risuko/") || dep === "risuko-cli" || dep === "risuko-js") {
        pkg[field][dep] = nextVersion;
      }
    }
  }
}

function ensureParent(filePath) {
  mkdirSync(dirname(filePath), { recursive: true });
}

function createPlaceholderFile(pkgName, filePath) {
  const fileName = basename(filePath);

  if (fileName.endsWith(".d.ts")) {
    writeFileSync(filePath, "export {};\n");
    return;
  }

  if (fileName.endsWith(".js")) {
    const shebang = fileName === "bin.js" ? "#!/usr/bin/env node\n\n" : "";
    writeFileSync(
      filePath,
      `${shebang}console.log(\"hello world (bootstrap placeholder for ${pkgName})\");\n`,
    );
    return;
  }

  if (fileName.endsWith(".node")) {
    writeFileSync(filePath, "hello world\n");
    return;
  }

  if (fileName.endsWith(".exe")) {
    writeFileSync(filePath, "hello world\r\n");
    return;
  }

  writeFileSync(filePath, "#!/usr/bin/env sh\necho \"hello world (bootstrap placeholder)\"\n");
}

function preparePackage(srcDir, outDir, nextVersion) {
  const pkg = readPkg(srcDir);
  pkg.version = nextVersion;
  if (pkg.scripts?.prepack) {
    delete pkg.scripts.prepack;
    if (Object.keys(pkg.scripts).length === 0) {
      delete pkg.scripts;
    }
  }
  updateInternalDeps(pkg, nextVersion);
  writePkg(outDir, pkg);

  const files = Array.isArray(pkg.files) ? pkg.files : [];
  for (const rel of files) {
    const abs = join(outDir, rel);
    ensureParent(abs);
    createPlaceholderFile(pkg.name, abs);
  }

  return pkg;
}

function packageExists(name, nextVersion) {
  const out = run("npm", ["view", `${name}@${nextVersion}`, "version"], root, true);
  return out === nextVersion;
}

const cliPlatformDirs = listPackageDirs("packages/risuko-cli/npm");
const jsPlatformDirs = listPackageDirs("packages/risuko-js/npm");
const mainDirs = [
  join(root, "packages/risuko-cli"),
  join(root, "packages/risuko-js"),
  join(root, "packages/risuko-app"),
];

const allSrcDirs = [...cliPlatformDirs, ...jsPlatformDirs, ...mainDirs];
const tempRoot = mkdtempSync(join(tmpdir(), "risuko-npm-bootstrap-"));

console.log(`Temp workspace: ${tempRoot}`);
console.log(`Version: ${version}`);
console.log(`Tag: ${tag}`);
console.log(`Dry run: ${dryRun ? "yes" : "no"}`);

if (!dryRun) {
  try {
    const whoami = run("npm", ["whoami"]);
    console.log(`npm auth ok as: ${whoami}`);
  } catch {
    console.error("npm auth missing. Run 'npm login' and retry.");
    rmSync(tempRoot, { recursive: true, force: true });
    process.exit(1);
  }
}

const prepared = [];

for (const srcDir of allSrcDirs) {
  const rel = srcDir.startsWith(root) ? srcDir.slice(root.length + 1) : srcDir;
  const outDir = join(tempRoot, rel);
  mkdirSync(outDir, { recursive: true });

  const pkg = preparePackage(srcDir, outDir, version);
  prepared.push({ name: pkg.name, version, dir: outDir, rel });
}

for (const item of prepared) {
  if (packageExists(item.name, item.version)) {
    console.log(`skip ${item.name}@${item.version} (already exists)`);
    continue;
  }

  if (dryRun) {
    console.log(`would publish ${item.name}@${item.version} from ${item.rel} (tag: ${tag})`);
    continue;
  }

  console.log(`publishing ${item.name}@${item.version} (tag: ${tag})`);
  run("npm", ["publish", "--access", "public", "--tag", tag], item.dir);
}

if (dryRun) {
  console.log("\nDry run completed.");
} else {
  console.log("\nBootstrap publish completed.");
}

console.log(`Temp workspace kept at: ${tempRoot}`);
