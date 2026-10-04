from src.infra.postgres.uow import PostgresUnitOfWork


class BaseController:
    def __init__(self, uow: PostgresUnitOfWork) -> None:
        self.uow = uow
