"""
msk_iam_auth.py — IAM token provider for connecting to MSK over SASL/IAM.

Mirrors producers/msk_iam_auth.py. Duplicated rather than imported across
the producers/consumers boundary so each package stays independently
deployable as its own container image.
"""

import asyncio

from aiokafka.abc import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


class MSKIAMTokenProvider(AbstractTokenProvider):
    def __init__(self, region: str):
        self.region = region

    async def token(self) -> str:
        loop = asyncio.get_running_loop()
        token, _expiry_ms = await loop.run_in_executor(
            None, MSKAuthTokenProvider.generate_auth_token, self.region
        )
        return token
