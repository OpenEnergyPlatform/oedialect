# 4. Example usage
from sqlalchemy import create_engine, text

import dummydialect  # noqa: must be imported so that registry.register is called

engine = create_engine("postgresql+dummydialect://test:test@localhost:5432")

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print(result.all())
