from pydantic import BaseSettings, SecretStr, Field


class Setup(BaseSettings):
    coinglass_api_key: SecretStr = Field(env='coinglass_api_key')

    class Config:
        env_file = '.env'


SETUP = Setup()
