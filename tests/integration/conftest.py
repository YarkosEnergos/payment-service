import asyncio
from collections.abc import AsyncIterator, Callable, Iterator
from functools import partial
from pathlib import Path

import docker
import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer

from src.infra.postgres.pg import get_db, get_session
from src.infra.postgres.uow import UnitOfWorkFactory, unit_of_work
from src.main import app

ROOT = Path(__file__).resolve().parents[2]
INTEGRATION_DIR = Path(__file__).resolve().parent


def docker_is_available() -> bool:
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    integration_items = [
        item for item in items if INTEGRATION_DIR in Path(item.path).parents
    ]
    if not integration_items:
        return
    skip = None if docker_is_available() else pytest.mark.skip("Docker is unavailable")
    for item in integration_items:
        item.add_marker(pytest.mark.integration)
        if skip is not None:
            item.add_marker(skip)


async def wait_for(
    predicate: Callable[[], bool],
    within: float = 10.0,
    interval: float = 0.05,
) -> None:
    for _ in range(int(within / interval)):
        if predicate():
            return
        await asyncio.sleep(interval)
    msg = f"Condition was not met within {within}s"
    raise TimeoutError(msg)


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:17-alpine", driver="asyncpg") as postgres:
        yield postgres.get_connection_url()


@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    with RabbitMqContainer(
        "rabbitmq:4.3-management",
        username="test",
        password="test",
    ) as rabbitmq:
        host = rabbitmq.get_container_host_ip()
        port = rabbitmq.get_exposed_port(rabbitmq.port)
        yield f"amqp://test:test@{host}:{port}/"


@pytest.fixture(scope="session")
async def session_factory(
    postgres_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", postgres_url)
    await asyncio.to_thread(command.upgrade, config, "head")

    engine = create_async_engine(postgres_url, poolclass=NullPool)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture
async def uow_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[UnitOfWorkFactory]:
    yield partial(unit_of_work, session_factory)

    async with session_factory() as db:
        await db.execute(text("TRUNCATE payments, outbox"))
        await db.commit()


@pytest.fixture
async def client(
    session_factory: async_sessionmaker[AsyncSession],
    uow_factory: UnitOfWorkFactory,
) -> AsyncIterator[AsyncClient]:
    async def override_get_session() -> AsyncIterator[AsyncSession]:
        async with get_db(session_factory) as db:
            yield db

    app.dependency_overrides[get_session] = override_get_session
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": "test-api-key"},
    ) as client:
        yield client
    app.dependency_overrides.clear()
