import os
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
from dotenv import load_dotenv

load_dotenv()

# Import models so Alembic can detect schema changes
from main import Base, DBReport, DBAuditLog, DBNotification
from geoalchemy2 import Geometry

config = context.config
config.set_main_option("sqlalchemy.url", os.getenv("DATABASE_URL"))

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def render_item(obj_type, obj, autogen_context):
    """Teach Alembic how to render GeoAlchemy2 Geometry columns."""
    if obj_type == "type" and isinstance(obj, Geometry):
        autogen_context.imports.add("from geoalchemy2 import Geometry")
        return f"Geometry('{obj.geometry_type}', srid={obj.srid}, spatial_index=True)"
    return False


def include_object(object, name, type_, reflected, compare_to):
    """Skip spatial indexes already managed by GeoAlchemy2."""
    if type_ == "index" and name and name.startswith("idx_"):
        return False
    return True


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_item=render_item,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_item=render_item,
            include_object=include_object,
            include_schemas=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
