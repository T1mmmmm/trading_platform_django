from django.conf import settings
from rest_framework import authentication, exceptions
from django.contrib.auth.models import AnonymousUser
from dataclasses import dataclass

@dataclass
class TenantUser:
    """
    轻量 user 对象：DRF 只要 user.is_authenticated 为 True 即可
    """
    tenant_id: str

    @property
    def is_authenticated(self) -> bool:
        return True

class ApiKeyAuthentication(authentication.BaseAuthentication):
    """
    Header: Authorization: Bearer <API_KEY>
    返回 (user, auth)
    """
    def authenticate(self, request):
        auth = request.headers.get("Authorization")
        if not auth:
            raise exceptions.AuthenticationFailed("Missing Authorization header")

        parts = auth.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise exceptions.AuthenticationFailed("Invalid Authorization header format")

        api_key = parts[1]
        tenant_id = settings.DEMO_API_KEYS.get(api_key)
        if not tenant_id:
            raise exceptions.AuthenticationFailed("Invalid API key")

        return (TenantUser(tenant_id=tenant_id), api_key)
