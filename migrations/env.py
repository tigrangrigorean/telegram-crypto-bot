import asyncio

from alembic import context

from bot.db import Base, engine


def migrate(conn):
    context.configure(connection=conn, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()


async def main():
    async with engine.connect() as conn:
        await conn.run_sync(migrate)
    await engine.dispose()


asyncio.run(main())
