#!/bin/sh
set -e

echo "Running Alembic migrations..."
alembic upgrade head || python -c "
from app.database import Base, engine
import asyncio
async def create():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
asyncio.run(create())
print('Tables created via SQLAlchemy.')
"

echo "Starting service..."
exec "$@"
