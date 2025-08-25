import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from dotenv import load_dotenv, find_dotenv

from db.models import Base

load_dotenv(find_dotenv())

db_url = (
    f'{os.getenv("DB_TYPE")}+{os.getenv("DB_ENGINE")}://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}\
@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'
)

engine = create_async_engine(db_url, echo=False)
async_session = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


if __name__ == "__main__":
    import asyncio

    asyncio.run(init_db())