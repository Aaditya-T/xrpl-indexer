const fs = require("fs");
const path = require("path");

const cwd = __dirname;
const python = process.env.XRPL_PYTHON || ["venv", ".venv"]
  .map((directory) => path.join(cwd, directory, "bin", "python"))
  .find((candidate) => fs.existsSync(candidate));

if (!python) {
  throw new Error("Python virtualenv not found. Expected venv/ or .venv/, or set XRPL_PYTHON.");
}

const shared = {
  cwd,
  script: python,
  interpreter: "none",
  autorestart: true,
  restart_delay: 2000,
  exp_backoff_restart_delay: 100,
  kill_timeout: 10000,
  time: true,
  log_date_format: "YYYY-MM-DDTHH:mm:ss.SSSZ",
  merge_logs: true,
};

module.exports = {
  apps: [
    {
      ...shared,
      name: "xrpl-indexer-v1",
      args: "main.py",
      max_memory_restart: "350M",
    },
    {
      ...shared,
      name: "xrpl-api",
      args: "-m uvicorn api:app --host 0.0.0.0 --port 8000 --workers 1",
      max_memory_restart: "220M",
    },
    {
      ...shared,
      name: "xrpl-monitor",
      args: "monitor.py",
      max_memory_restart: "80M",
    },
  ],
};
