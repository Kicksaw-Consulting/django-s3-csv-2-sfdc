from django.conf import settings
from simple_salesforce import Salesforce


class SfClient(Salesforce):
    def __init__(self):
        config = {
            "username": settings.SFDC_USERNAME,
            "password": settings.SFDC_PASSWORD,
            "security_token": settings.SFDC_SECURITY_TOKEN,
        }
        if settings.SFDC_DOMAIN.lower() != "na":
            config["domain"] = settings.SFDC_DOMAIN

        super().__init__(**config)