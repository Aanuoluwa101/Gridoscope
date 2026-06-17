"""
msk_iam_auth.py — IAM token provider for connecting to MSK over SASL/IAM.

aiokafka's OAUTHBEARER mechanism expects an async token() method, but the
AWS signer library is synchronous (it calls out to STS via boto3). We bridge
the two with run_in_executor so signing a token doesn't block the event loop
that's also driving 500 meter coroutines.

Only used when KAFKA_SECURITY_PROTOCOL=SASL_SSL — local dev against the
docker-compose broker stays on PLAINTEXT and never imports this.
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
