#!/usr/bin/env python3
"""
Dashboard HTTP server for PumpFun Bot.
Serves dashboard.html at http://localhost:8765/ and provides API endpoints.

Usage:
    python bot_server.py
    /Users/valentyn/.local/bin/uv run bot_server.py
"""

import json
import os
import re
import subprocess
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).parent.resolve()
CONFIG_FILE = PROJECT_ROOT / "bot_config.json"
ENV_FILE = PROJECT_ROOT / ".env"
DASHBOARD_FILE = PROJECT_ROOT / "dashboard.html"
SCANNER_SCRIPT = PROJECT_ROOT / "src" / "scanner_runner.py"
BOT_CONFIG_YAML = "bots/bot-scanner-telegram.yaml"

UV_PATH = Path.home() / ".local" / "bin" / "uv"

DEFAULT_CONFIG = {
    "active_preset": 1,
    "max_concurrent_positions": 1,
    "open_positions": 0,
    "auto_trading": False,
    "test_mode": False,
    "mode": "infinite",
    "max_trades": 10,
    "stats": {
        "tokens_found_today": 0,
        "tokens_passed_filters": 0,
        "buys_executed": 0,
        "test_buys_executed": 0,
        "test_wins": 0,
        "test_losses": 0,
        "test_total_pnl_sol": 0.0,
        "real_wins": 0,
        "real_losses": 0,
        "real_total_pnl_sol": 0.0,
    },
    "presets": {
        "1": {
            "name": "Preset 1",
            "buy_amount_sol": 0.01,
            "priority_fee_sol": 0.001,
            "jito_tip_sol": 0.003,
            "gas_fee_sol": 0.000005,
            "buy_slippage": 30,
            "sell_slippage": 25,
            "max_retries": 1,
            "take_profits": [{"price_pct": 50, "position_pct": 50}],
            "stop_losses": [{"price_pct": 30, "position_pct": 100}],
            "trailing_stops": [],
            "filters": {"min_dev_buy_sol": 0.1, "dev_buy_check_enabled": False, "min_ath_last5": 0, "ath_require_all": False, "min_migrations_last5": 0, "min_tx_count": 0, "max_tx_count": 0, "tx_count_require_all": False, "min_lifetime_minutes": 0, "lifetime_require_all": False, "min_entry_mc_usd": 0, "max_entry_mc_usd": 0},
        },
        "2": {
            "name": "Preset 2",
            "buy_amount_sol": 0.02,
            "priority_fee_sol": 0.001,
            "jito_tip_sol": 0.003,
            "gas_fee_sol": 0.000005,
            "buy_slippage": 30,
            "sell_slippage": 25,
            "max_retries": 1,
            "take_profits": [],
            "stop_losses": [],
            "trailing_stops": [],
            "filters": {"min_dev_buy_sol": 0.5, "dev_buy_check_enabled": True, "min_ath_last5": 0, "ath_require_all": False, "min_migrations_last5": 0, "min_tx_count": 0, "max_tx_count": 0, "tx_count_require_all": False, "min_lifetime_minutes": 0, "lifetime_require_all": False, "min_entry_mc_usd": 0, "max_entry_mc_usd": 0},
        },
        "3": {
            "name": "Preset 3",
            "buy_amount_sol": 0.05,
            "priority_fee_sol": 0.002,
            "jito_tip_sol": 0.005,
            "gas_fee_sol": 0.000005,
            "buy_slippage": 30,
            "sell_slippage": 25,
            "max_retries": 1,
            "take_profits": [],
            "stop_losses": [],
            "trailing_stops": [],
            "filters": {"min_dev_buy_sol": 1.0, "dev_buy_check_enabled": True, "min_ath_last5": 20000, "ath_require_all": False, "min_migrations_last5": 1, "min_tx_count": 0, "max_tx_count": 0, "tx_count_require_all": False, "min_lifetime_minutes": 0, "lifetime_require_all": False, "min_entry_mc_usd": 0, "max_entry_mc_usd": 0},
        },
    },
}

_config_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def load_config() -> dict:
    with _config_lock:
        if CONFIG_FILE.exists():
            try:
                with open(CONFIG_FILE) as f:
                    return json.load(f)
            except Exception:
                pass
        return json.loads(json.dumps(DEFAULT_CONFIG))


def save_config(data: dict) -> None:
    with _config_lock:
        with open(CONFIG_FILE, "w") as f:
            json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# .env helpers
# ---------------------------------------------------------------------------


def load_env() -> dict:
    env: dict = {}
    if not ENV_FILE.exists():
        return env
    with open(ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                env[key.strip()] = val.strip()
    return env


def save_env_key(key: str, value: str) -> None:
    content = ENV_FILE.read_text() if ENV_FILE.exists() else ""
    pattern = rf"^{re.escape(key)}=.*$"
    replacement = f"{key}={value}"
    if re.search(pattern, content, re.MULTILINE):
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    else:
        content = content.rstrip("\n") + f"\n{replacement}\n"
    ENV_FILE.write_text(content)


# ---------------------------------------------------------------------------
# Wallet helpers
# ---------------------------------------------------------------------------


def get_public_key() -> str | None:
    env = load_env()
    private_key = env.get("SOLANA_PRIVATE_KEY", "")
    if not private_key:
        return None
    try:
        import base58
        from solders.keypair import Keypair

        kp = Keypair.from_bytes(base58.b58decode(private_key))
        return str(kp.pubkey())
    except Exception:
        return None


def get_sol_balance(pubkey: str) -> float | None:
    env = load_env()
    rpc = env.get("SOLANA_NODE_RPC_ENDPOINT", "")
    if not rpc or not pubkey:
        return None
    payload = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [pubkey, {"commitment": "confirmed"}],
    }).encode()
    try:
        req = urllib.request.Request(
            rpc,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        return data["result"]["value"] / 1_000_000_000
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Bot process control
# ---------------------------------------------------------------------------


def is_bot_running() -> bool:
    """Check if scanner_runner.py process is running (any launch method)."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "scanner_runner.py"],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0
    except Exception:
        return False


def start_bot() -> tuple[bool, str]:
    """Kill any existing instance then start a fresh one."""
    # Stop any existing instance first
    subprocess.run(["pkill", "-f", "scanner_runner.py"], capture_output=True)

    # Reset stats
    cfg = load_config()
    cfg["stats"] = {
        "tokens_found_today": 0,
        "tokens_passed_filters": 0,
        "buys_executed": 0,
        "test_buys_executed": 0,
        "test_wins": 0,
        "test_losses": 0,
        "test_total_pnl_sol": 0.0,
        "real_wins": 0,
        "real_losses": 0,
        "real_total_pnl_sol": 0.0,
    }
    cfg["open_positions"] = 0
    save_config(cfg)

    try:
        proc = subprocess.Popen(
            [str(UV_PATH), "run", str(SCANNER_SCRIPT), BOT_CONFIG_YAML],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True, f"Bot started (PID {proc.pid})"
    except Exception as e:
        return False, str(e)


def stop_bot() -> tuple[bool, str]:
    """Kill scanner_runner.py by name."""
    try:
        result = subprocess.run(
            ["pkill", "-f", "scanner_runner.py"],
            capture_output=True,
        )
        if result.returncode == 0:
            return True, "Bot stopped"
        return False, "Bot was not running"
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # suppress access log
        pass

    def _send_json(self, data: dict, status: int = 200) -> None:
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        return json.loads(self.rfile.read(length))

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/":
            if DASHBOARD_FILE.exists():
                body = DASHBOARD_FILE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self._send_json({"error": "dashboard.html not found"}, 404)

        elif path == "/api/status":
            cfg = load_config()
            pubkey = get_public_key()
            self._send_json({
                "running": is_bot_running(),
                "pubkey": pubkey,
                "stats": cfg.get("stats", {}),
                "open_positions": cfg.get("open_positions", 0),
                "max_concurrent_positions": cfg.get("max_concurrent_positions", 1),
            })

        elif path == "/api/config":
            self._send_json(load_config())

        elif path == "/api/balance":
            pubkey = get_public_key()
            balance = get_sol_balance(pubkey) if pubkey else None
            self._send_json({"balance": balance, "pubkey": pubkey})

        elif path == "/api/wallet":
            pubkey = get_public_key()
            self._send_json({"pubkey": pubkey})

        else:
            self._send_json({"error": "not found"}, 404)

    def do_POST(self):
        path = urlparse(self.path).path

        if path == "/api/start":
            ok, msg = start_bot()
            self._send_json({"success": ok, "message": msg})

        elif path == "/api/stop":
            ok, msg = stop_bot()
            # Always reset counters/stats — even if bot was already stopped,
            # this clears any stuck open_positions from a crashed session.
            cfg = load_config()
            cfg["stats"] = {
                "tokens_found_today": 0,
                "tokens_passed_filters": 0,
                "buys_executed": 0,
                "test_buys_executed": 0,
                "test_wins": 0,
                "test_losses": 0,
                "test_total_pnl_sol": 0.0,
                "real_wins": 0,
                "real_losses": 0,
                "real_total_pnl_sol": 0.0,
            }
            cfg["open_positions"] = 0
            save_config(cfg)
            self._send_json({"success": ok, "message": msg})

        elif path == "/api/reset-positions":
            cfg = load_config()
            cfg["open_positions"] = 0
            save_config(cfg)
            self._send_json({"success": True, "message": "Positions reset to 0"})

        elif path == "/api/config":
            try:
                data = self._read_body()
                save_config(data)
                self._send_json({"success": True})
            except Exception as e:
                self._send_json({"success": False, "error": str(e)}, 400)

        elif path == "/api/save-key":
            try:
                data = self._read_body()
                key = data.get("key", "").strip()
                if not key:
                    self._send_json({"success": False, "error": "Empty key"}, 400)
                    return
                save_env_key("SOLANA_PRIVATE_KEY", key)
                pubkey = get_public_key()
                self._send_json({"success": True, "pubkey": pubkey})
            except Exception as e:
                self._send_json({"success": False, "error": str(e)}, 400)

        else:
            self._send_json({"error": "not found"}, 404)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    if not CONFIG_FILE.exists():
        save_config(json.loads(json.dumps(DEFAULT_CONFIG)))
        print(f"Created {CONFIG_FILE.name}")

    port = 8765
    server = HTTPServer(("localhost", port), Handler)
    print(f"Dashboard → http://localhost:{port}/")
    print("Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
