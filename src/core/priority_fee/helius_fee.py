"""Helius getPriorityFeeEstimate integration."""
import aiohttp

from utils.logger import get_logger

logger = get_logger(__name__)


def _safe_url(url: str) -> str:
    if "api-key=" in url:
        return url.split("api-key=", 1)[0] + "api-key=[REDACTED]"
    return url


class HeliusFeeEstimator:
    """Fetch veryHigh priority fee estimate from Helius."""

    PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

    async def get_max_fee_microlamports(self, helius_url: str) -> int | None:
        """Return priorityFeeLevels.veryHigh in µL/CU, or None on any error.

        Caller MUST enforce a hard cap — veryHigh can exceed 4,000,000 µL/CU.
        Timeout: 3 seconds.  API key is redacted from all log output.
        """
        payload = {
            "jsonrpc": "2.0",
            "id": "fee",
            "method": "getPriorityFeeEstimate",
            "params": [{
                "accountKeys": [self.PUMP_FUN_PROGRAM],
                "options": {"includeAllPriorityFeeLevels": True},
            }],
        }
        try:
            timeout = aiohttp.ClientTimeout(total=3.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    helius_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                ) as resp:
                    data = await resp.json(content_type=None)

            levels = (data.get("result") or {}).get("priorityFeeLevels") or {}
            val = levels.get("veryHigh")
            if val is not None:
                return int(val)
            logger.warning(f"Helius fee: veryHigh not in response: {list(levels.keys())}")
            return None

        except Exception as e:
            logger.warning(f"Helius fee estimate failed: {e} (url={_safe_url(helius_url)})")
            return None
