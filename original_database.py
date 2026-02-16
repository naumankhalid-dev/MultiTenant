"""
Database connection manager with master-replica support
"""
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
    AsyncEngine)
from sqlalchemy import text
from structlog import get_logger

logger = get_logger(__name__)


class Database:
    """Database connection manager with master-replica support"""

    def __init__(
        self,
        master_config: Dict[str, Any],
        replica_config: Dict[str, Any],
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
    ):
        self.master_config = master_config
        self.replica_config = replica_config

        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle

        self.master_engine: Optional[AsyncEngine] = None
        self.replica_engine: Optional[AsyncEngine] = None

        self.write_session_factory: Optional[async_sessionmaker] = None
        self.read_session_factory: Optional[async_sessionmaker] = None
        self._connected = False


    def _build_connection_string(self, config: Dict[str, Any]) -> str:
        # Use asyncpg driver (fast & mature for async PostgreSQL)
        return (
            f"postgresql+asyncpg://{config['username']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )

    async def connect(self) -> None:
        try:
            logger.info("Connecting to databases...")

            # ---------- MASTER ----------
            master_url = self._build_connection_string(self.master_config)
            self.master_engine = create_async_engine(
                master_url,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_timeout=self.pool_timeout,
                pool_recycle=self.pool_recycle,
                echo=False,
            )

            # ---------- REPLICA ----------
            if all(self.replica_config.values()):
                try:
                    replica_url = self._build_connection_string(self.replica_config)
                    self.replica_engine = create_async_engine(
                        replica_url,
                        pool_size=self.pool_size,
                        max_overflow=self.max_overflow,
                        pool_timeout=self.pool_timeout,
                        pool_recycle=self.pool_recycle,
                        echo=False,
                    )
                    logger.info("Replica engine created")
                except Exception as e:
                    logger.warning(f"Replica unavailable: {e}")
                    self.replica_engine = None
            else:
                logger.info("Replica config missing; using master only")

            # ---------- SESSION FACTORIES ----------
            # Write operations always use master
            self.write_session_factory = async_sessionmaker(
                bind=self.master_engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            # Read operations use replica if available, otherwise master
            read_engine = self.replica_engine if self.replica_engine else self.master_engine
            self.read_session_factory = async_sessionmaker(
                bind=read_engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            logger.info(f"Session factories created - Reads: {'replica' if self.replica_engine else 'master'}, Writes: master")

            # ---------- TEST CONNECTIONS ----------
            await self._test_connections()

            self._connected = True
            logger.info("Database connected successfully")

        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    async def disconnect(self) -> None:
        logger.info("Disconnecting databases...")

        if self.master_engine:
            await self.master_engine.dispose()

        if self.replica_engine:
            await self.replica_engine.dispose()

        self._connected = False
        logger.info("Database disconnected")

    async def _test_connections(self) -> None:
        async with self.master_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
            logger.debug("Master OK")

        if self.replica_engine:
            try:
                async with self.replica_engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
                    logger.debug("Replica OK")
            except Exception:
                logger.warning("Replica failed, falling back to master")
                self.replica_engine = None

    def health_check(self) -> Dict[str, Any]:
        return {
            "status": "healthy" if self._connected else "unhealthy",
            "master": bool(self.master_engine),
            "replica": bool(self.replica_engine),
        }

    def get_session(self, operation_type: str = "write") -> AsyncSession:
        """
        Get a database session based on operation type.
        
        Args:
            operation_type: "read" for read operations (uses replica if available),
                          "write" for write operations (always uses master)
        
        Returns:
            AsyncSession configured for the specified operation type
        """
        if not self._connected:
            raise RuntimeError("Database not connected")

        if operation_type == "read":
            return self.read_session_factory()
        else:
            return self.write_session_factory()